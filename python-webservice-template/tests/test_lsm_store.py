from persistence.stream_store.lsm_store import LSMStore
from tests.publishers.flight_server import make_batch


def _run_row_count(store: LSMStore) -> int:
    # Rows physically retained across compacted runs (memtable excluded). The only
    # way to observe tombstone reclamation, which has no HTTP-visible signal.
    return sum(run.height for run in store._snapshot.runs)


def test_compaction_drops_tombstones():
    store = LSMStore(flush_rows=1, compaction_runs=2)   # flush each batch; compact at 2 runs
    store.ingest(make_batch([(1, "a", "v1", "upsert"), (2, "b", "v1", "upsert")]))  # run 1
    store.ingest(make_batch([(2, "b", "v1", "delete")]))                            # run 2 -> compaction

    rows, total = store.query(limit=100)
    assert total == 1
    assert {r["id"] for r in rows} == {1}          # id=2 deleted, absent from reads
    assert _run_row_count(store) == 1              # tombstone for id=2 physically reclaimed
