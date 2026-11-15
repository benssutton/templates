"""In-memory LSM (Log-Structured Merge) store for Arrow record batches.

SINGLE-WRITER CONTRACT — LSMStore has no internal locking. It is designed
to be written by exactly one thread (the ingest consumer thread started by
StreamIngestService). Concurrent calls to `ingest()` produce undefined
behaviour. Readers (`query()`) are safe to call from any thread or coroutine
because `_publish()` swaps the snapshot reference atomically at the end of
every write; readers always see a consistent, immutable snapshot.

Lifecycle: memtable → run (flush) → compacted run.  When the number of runs
reaches `compaction_runs`, all runs are merged into one. Compaction is the
only point at which delete tombstones are reclaimed; a tombstone in the
memtable or an un-compacted run continues to shadow older versions of the
same key across all in-memory frames until a full merge has occurred.
"""

from dataclasses import dataclass

import pyarrow as pa
import polars as pl

ORDER_COLUMN = "seqno"


class InvalidBatchError(ValueError):
    """The batch cannot be merged (missing key/op columns). Retrying the same
    batch can never succeed, so transports must reject it, not redeliver it."""


def _merge_frame(
    frames: tuple[pl.DataFrame, ...], key_columns: list[str]
) -> pl.DataFrame | None:
    if not frames:
        return None
    combined = pl.concat(frames, how="vertical")
    # Window function: rank rows within each key partition by recency
    # (newest seqno first). key_columns is the single extension point:
    # ["id"] today, ["id", "version"] later, with no other change.
    winners = (
        combined.with_columns(
            pl.col(ORDER_COLUMN)
            .rank("ordinal", descending=True)
            .over(key_columns)
            .alias("_rn")
        )
        .filter(pl.col("_rn") == 1)
        .drop("_rn")
    )
    return winners


def _merge_to_rows(
    frames: tuple[pl.DataFrame, ...], key_columns: list[str], limit: int | None
) -> tuple[list[dict], int]:
    winners = _merge_frame(frames, key_columns)
    if winners is None:
        return [], 0
    live = winners.filter(pl.col("op") != "delete").sort(key_columns)
    total = live.height
    if limit:
        live = live.head(limit)
    return live.drop([ORDER_COLUMN, "op"]).to_dicts(), total


@dataclass(frozen=True)
class _Snapshot:
    runs: tuple[pl.DataFrame, ...]
    memtable: tuple[pl.DataFrame, ...]


class LSMStore:
    def __init__(
        self,
        flush_rows: int,
        compaction_runs: int,
        key_columns: list[str] | None = None,
    ) -> None:
        self._flush_rows = flush_rows
        self._compaction_runs = compaction_runs
        self._key_columns = key_columns or ["id"]
        self._seqno = 0
        # writer-private working set (only the ingest thread touches these):
        self._memtable: list[pl.DataFrame] = []
        self._memtable_rows = 0
        self._runs: list[pl.DataFrame] = []
        self._snapshot = _Snapshot(runs=(), memtable=())

    def validate(self, batch: pa.RecordBatch) -> None:
        """Raise InvalidBatchError if ingest() would refuse this batch. Pure (no
        state change), so callers can check a whole message before writing any of it."""
        required = [*self._key_columns, "op"]
        missing = [c for c in required if c not in batch.schema.names]
        if missing:
            # Validate up front: an un-keyed frame would be accepted here and only
            # fail later inside compaction, poisoning every subsequent query.
            raise InvalidBatchError(f"batch missing required columns: {missing}")

    def ingest(self, batch: pa.RecordBatch) -> None:
        self.validate(batch)
        frame = pl.from_arrow(batch)
        # from_arrow() is typed DataFrame | Series; a RecordBatch always yields a DataFrame.
        assert isinstance(frame, pl.DataFrame)
        n = frame.height
        frame = frame.with_columns(
            pl.Series(ORDER_COLUMN, range(self._seqno, self._seqno + n))
        )
        self._seqno += n
        self._memtable.append(frame)
        self._memtable_rows += n
        if self._memtable_rows >= self._flush_rows:
            self._flush()
            if len(self._runs) >= self._compaction_runs:
                self._compact()
        self._publish()

    def _flush(self) -> None:
        if not self._memtable:
            return
        self._runs.append(pl.concat(self._memtable, how="vertical"))
        self._memtable = []
        self._memtable_rows = 0

    def _compact(self) -> None:
        merged = _merge_frame(tuple(self._runs), self._key_columns)
        if merged is None:
            self._runs = []
            return
        # Full compaction collapses every run into one, so a delete tombstone has
        # finished shadowing older versions and can be reclaimed here. A later
        # re-insert of the same key arrives with a higher seqno and wins anyway.
        merged = merged.filter(pl.col("op") != "delete")
        self._runs = [merged] if merged.height > 0 else []

    def _publish(self) -> None:
        self._snapshot = _Snapshot(
            runs=tuple(self._runs), memtable=tuple(self._memtable)
        )

    def query(self, limit: int) -> tuple[list[dict], int]:
        snap = self._snapshot  # atomic read, no lock
        return _merge_to_rows(snap.runs + snap.memtable, self._key_columns, limit)
