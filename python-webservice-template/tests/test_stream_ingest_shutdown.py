"""StreamIngestService must never wait on the ingest thread's OS handle:
Thread.join/is_alive can block indefinitely on Windows while the OS thread
exits. Liveness and shutdown go through an event set when the loop returns."""

import asyncio
import logging
import threading
import time

from ingestion.base import ConnectionState, OnBound
from persistence.stream_store.lsm_store import LSMStore
from services.stream_ingest import StreamIngestService
from settings import Settings
from tests.support.wait import eventually


class _ReturningConsumer:
    """deliveries() returns at once, so the ingest loop returns at once."""

    def deliveries(self, on_bound: OnBound | None = None):
        yield from ()

    def close(self) -> None:
        pass

    def connection_state(self) -> ConnectionState:
        return ConnectionState.DOWN


class _StuckConsumer(_ReturningConsumer):
    """deliveries() ignores close() and blocks until the test releases it."""

    def __init__(self) -> None:
        self.release = threading.Event()

    def deliveries(self, on_bound: OnBound | None = None):
        self.release.wait()
        yield from ()


def _settings(**overrides) -> Settings:
    return Settings(
        ingest_transport="noop",
        ingest_max_disconnect_seconds=None,
        ingest_max_consecutive_failures=0,
        **overrides,
    )


def _store() -> LSMStore:
    return LSMStore(flush_rows=100, compaction_runs=4)


def _forbid_os_waits(thread: threading.Thread) -> None:
    def forbidden(*_: object) -> None:
        raise AssertionError("waited on the ingest thread's OS handle")

    thread.join = forbidden  # type: ignore[method-assign]
    thread.is_alive = forbidden  # type: ignore[method-assign]


async def test_liveness_and_shutdown_do_not_wait_on_the_os_thread():
    svc = StreamIngestService(_ReturningConsumer(), _store(), _settings())
    async with svc:
        assert svc._thread is not None
        _forbid_os_waits(svc._thread)
        await eventually(
            lambda: not svc.health_check().thread_alive,
            timeout=5,
            what="thread_alive False once the loop returned",
        )
    assert svc.health_check().thread_alive is False


async def test_shutdown_gives_up_after_the_join_timeout(caplog):
    consumer = _StuckConsumer()
    svc = StreamIngestService(
        consumer, _store(), _settings(ingest_join_timeout_seconds=0.2)
    )
    try:
        with caplog.at_level(logging.ERROR, logger="services.stream_ingest"):
            async with svc:
                assert svc.health_check().thread_alive is True
                start = time.monotonic()
            elapsed = time.monotonic() - start
        assert elapsed < 3.0
        assert any("did not stop within" in r.getMessage() for r in caplog.records)
    finally:
        consumer.release.set()
        await asyncio.sleep(0)
