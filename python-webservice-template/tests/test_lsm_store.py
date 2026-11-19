from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
import pytest

from persistence.stream_store.lsm_store import InvalidBatchError, LSMStore
from tests.publishers.batches import make_batch


def _run_row_count(store: LSMStore) -> int:
    # Rows physically retained across compacted runs (memtable excluded). The only
    # way to observe tombstone reclamation, which has no HTTP-visible signal.
    return sum(run.height for run in store._snapshot.runs)


def test_compaction_drops_tombstones():
    store = LSMStore(
        flush_rows=1, compaction_runs=2
    )  # flush each batch; compact at 2 runs
    store.ingest(
        make_batch([(1, "a", "v1", "upsert"), (2, "b", "v1", "upsert")])
    )  # run 1
    store.ingest(make_batch([(2, "b", "v1", "delete")]))  # run 2 -> compaction

    rows, total = store.query(limit=100)
    assert total == 1
    assert {r["id"] for r in rows} == {1}  # id=2 deleted, absent from reads
    assert _run_row_count(store) == 1  # tombstone for id=2 physically reclaimed


def test_concurrent_ingest_loses_no_rows():
    # HTTP ingest (thread pool) and the transport thread both write, so the store
    # must serialise writers itself. Small flush/compaction thresholds make the
    # memtable swap in _flush race against appends if writes are not serialised.
    store = LSMStore(flush_rows=3, compaction_runs=2)
    writers, batches_per_writer, rows_per_batch = 8, 50, 2

    def write(writer: int) -> None:
        for b in range(batches_per_writer):
            base = (writer * batches_per_writer + b) * rows_per_batch
            store.ingest(
                make_batch(
                    [(base + i, "n", "v", "upsert") for i in range(rows_per_batch)]
                )
            )

    with ThreadPoolExecutor(max_workers=writers) as pool:
        list(pool.map(write, range(writers)))

    _, total = store.query(limit=1)
    assert total == writers * batches_per_writer * rows_per_batch
    assert store._seqno == total  # every row got its own sequence number


def test_ingest_rejects_batch_missing_key_column():
    store = LSMStore(flush_rows=100, compaction_runs=4, key_columns=["id"])
    batch = pa.record_batch({"name": ["x"], "value": ["v"], "op": ["upsert"]})
    with pytest.raises(InvalidBatchError, match=r"missing required columns: \['id'\]"):
        store.ingest(batch)
    assert store.query(100) == ([], 0)  # nothing was written


def test_ingest_rejects_batch_missing_op_column():
    store = LSMStore(flush_rows=100, compaction_runs=4, key_columns=["id"])
    batch = pa.record_batch(
        {"id": pa.array([1], pa.int64()), "name": ["x"], "value": ["v"]}
    )
    with pytest.raises(InvalidBatchError, match=r"\['op'\]"):
        store.ingest(batch)


def _typed_batch(**columns: pa.Array) -> pa.RecordBatch:
    return pa.record_batch(
        {
            "id": pa.array([1], pa.int64()),
            "name": pa.array(["x"]),
            "value": pa.array(["v"]),
            "op": pa.array(["upsert"]),
            **columns,
        }
    )


@pytest.mark.parametrize(
    "conflicting",
    [
        _typed_batch(extra=pa.array([1], pa.int64())),  # extra column
        _typed_batch(value=pa.array([7], pa.int64())),  # different dtype
    ],
    ids=["extra-column", "different-dtype"],
)
def test_schema_conflicting_batch_is_rejected_and_store_keeps_working(conflicting):
    # flush_rows=1 flushes and compacts on every write, so a conflicting frame
    # that slipped into the memtable would break the next concat.
    store = LSMStore(flush_rows=1, compaction_runs=2)
    store.ingest(make_batch([(1, "a", "v1", "upsert")]))  # fixes the schema
    with pytest.raises(InvalidBatchError, match="schema"):
        store.validate(conflicting)
    with pytest.raises(InvalidBatchError, match="schema"):
        store.ingest(conflicting)
    store.ingest(make_batch([(2, "b", "v2", "upsert")]))
    rows, total = store.query(limit=10)
    assert total == 2
    assert [r["id"] for r in rows] == [1, 2]
