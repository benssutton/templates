"""StreamIngestService decides HOW each delivery is settled. Driven through the
real ingest loop with a scripted (non-mock) consumer and real LSMStores."""

import threading
import time

import pyarrow as pa

from ingestion.base import ConnectionState, Delivery
from persistence.stream_store.lsm_store import LSMStore
from services.stream_ingest import StreamIngestService
from settings import Settings
from tests.publishers.batches import make_batch
from tests.support.wait import eventually


class _ScriptedConsumer:
    """Yields the given deliveries, then idles until close()."""

    def __init__(self, deliveries: list[Delivery]) -> None:
        self._deliveries = deliveries
        self._closed = threading.Event()

    def deliveries(self):
        yield from self._deliveries
        self._closed.wait()

    def close(self) -> None:
        self._closed.set()

    def connection_state(self) -> ConnectionState:
        return ConnectionState.CONNECTED


class _FailingStore(LSMStore):
    """A real store whose write raises: stands in for an unexpected,
    possibly transient storage error."""

    def ingest(self, batch: pa.RecordBatch) -> None:
        raise RuntimeError("simulated storage failure")


def _delivery(batches: list[pa.RecordBatch], outcomes: list[str]) -> Delivery:
    return Delivery(
        batches=batches,
        accept=lambda: outcomes.append("accept"),
        fail=lambda: outcomes.append("fail"),
        reject=lambda: outcomes.append("reject"),
    )


async def _settle_one(
    batches, *, store=None, **settings_overrides
) -> tuple[list[str], LSMStore]:
    outcomes: list[str] = []
    store = store or LSMStore(flush_rows=100, compaction_runs=4)
    settings = Settings(
        ingest_transport="noop",
        ingest_max_disconnect_seconds=None,
        **settings_overrides,
    )
    svc = StreamIngestService(
        _ScriptedConsumer([_delivery(batches, outcomes)]), store, settings
    )
    async with svc:
        await eventually(lambda: bool(outcomes), timeout=5, what="delivery settled")
    return outcomes, store


async def test_successful_write_accepts():
    outcomes, store = await _settle_one(
        [
            make_batch([(1, "a", "v1", "upsert")]),
            make_batch([(2, "b", "v1", "upsert")]),
        ]
    )
    assert outcomes == ["accept"]  # settled ONCE, after every batch in the message
    assert store.query(100)[1] == 2


async def test_unmergeable_batch_rejects():
    bad = pa.record_batch({"name": ["x"], "value": ["v"], "op": ["upsert"]})
    outcomes, _ = await _settle_one([bad])
    assert outcomes == ["reject"]


async def test_oversized_batch_rejects():
    outcomes, store = await _settle_one(
        [make_batch([(1, "a", "v1", "upsert")])], max_ingest_batch_bytes=1
    )
    assert outcomes == ["reject"]
    assert store.query(100)[1] == 0


async def test_unexpected_store_error_fails_for_redelivery():
    outcomes, _ = await _settle_one(
        [make_batch([(1, "a", "v1", "upsert")])],
        store=_FailingStore(flush_rows=100, compaction_runs=4),
    )
    assert outcomes == ["fail"]


def _service(deliveries: list[Delivery], store: LSMStore, **settings_overrides):
    settings = Settings(
        ingest_transport="noop",
        ingest_max_disconnect_seconds=None,
        **settings_overrides,
    )
    return StreamIngestService(_ScriptedConsumer(deliveries), store, settings)


async def test_raising_reject_is_not_followed_by_fail():
    """Each delivery is settled at most once: a settle callback that raises
    propagates to the loop's handler instead of triggering a second outcome."""
    outcomes: list[str] = []

    def _raising_reject() -> None:
        outcomes.append("reject")
        raise RuntimeError("flow terminated")

    oversized = Delivery(
        batches=[make_batch([(1, "a", "v1", "upsert")])],
        accept=lambda: outcomes.append("accept"),
        fail=lambda: outcomes.append("fail"),
        reject=_raising_reject,
    )
    nxt = _delivery([make_batch([(2, "b", "v1", "upsert")])], outcomes)
    store = LSMStore(flush_rows=100, compaction_runs=4)
    async with _service([oversized, nxt], store, max_ingest_batch_bytes=1):
        await eventually(lambda: len(outcomes) >= 2, timeout=5, what="both settled")
    # The loop carried on to the next delivery (also oversized -> reject).
    assert outcomes == ["reject", "reject"]


async def test_persistent_store_failure_backs_off_between_fails():
    """fail() makes the broker redeliver at once; without a settlement backoff
    a persistent store error would spin. Delays double: ~1 s, then ~2 s."""
    fail_times: list[float] = []

    def _d() -> Delivery:
        return Delivery(
            batches=[make_batch([(1, "a", "v1", "upsert")])],
            accept=lambda: None,
            fail=lambda: fail_times.append(time.monotonic()),
            reject=lambda: None,
        )

    store = _FailingStore(flush_rows=100, compaction_runs=4)
    async with _service([_d(), _d(), _d()], store):
        await eventually(lambda: len(fail_times) >= 3, timeout=5, what="three fails")
    assert fail_times[1] - fail_times[0] >= 1.0
    assert fail_times[2] - fail_times[1] >= 2.0


async def test_close_during_settlement_backoff_is_prompt():
    fail_times: list[float] = []

    def _d() -> Delivery:
        return Delivery(
            batches=[make_batch([(1, "a", "v1", "upsert")])],
            accept=lambda: None,
            fail=lambda: fail_times.append(time.monotonic()),
            reject=lambda: None,
        )

    store = _FailingStore(flush_rows=100, compaction_runs=4)
    svc = _service([_d(), _d(), _d()], store)
    async with svc:
        await eventually(lambda: len(fail_times) >= 2, timeout=5, what="two fails")
        started = time.monotonic()  # a ~2 s backoff is now pending
    assert time.monotonic() - started < 1.0
    assert len(fail_times) == 2  # the pending delivery was not processed
