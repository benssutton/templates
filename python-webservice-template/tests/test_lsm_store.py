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
