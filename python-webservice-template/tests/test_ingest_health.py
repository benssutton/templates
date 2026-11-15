import asyncio
import logging
import threading
from typing import cast

from pydantic import SecretStr
from solace.messaging.messaging_service import ServiceEvent

from ingestion.base import ConnectionState
from ingestion.solace.client import SolaceBatchConsumer, _StateListener
from persistence.stream_store.lsm_store import LSMStore
from services.stream_ingest import StreamIngestService
from settings import Settings
from tests.publishers.batches import make_batch
from tests.support.settings import solace_overrides
from tests.support.solace import broker_paused, create_endpoint_denied_user
from tests.support.wait import eventually


def test_solace_consumer_state_transitions_via_listeners():
    consumer = SolaceBatchConsumer(Settings())
    assert consumer.connection_state() == ConnectionState.DOWN

    listener = _StateListener(consumer)
    # The listeners ignore the event payload; a real ServiceEvent needs a live broker.
    no_event = cast(ServiceEvent, None)
    listener.on_reconnecting(no_event)
    assert consumer.connection_state() == ConnectionState.RECONNECTING
    listener.on_reconnected(no_event)
    assert consumer.connection_state() == ConnectionState.CONNECTED
    listener.on_service_interrupted(no_event)
    assert consumer.connection_state() == ConnectionState.DOWN


class _FakeConsumer:
    """Real (non-mock) BatchConsumer whose state is fixed and whose deliveries()
    blocks until close(), so the ingest thread stays alive during tests."""

    def __init__(self, state: ConnectionState) -> None:
        self._state = state
        self._closed = threading.Event()

    def deliveries(self):
        self._closed.wait()
        return
        yield  # make this a generator

    def close(self) -> None:
        self._closed.set()

    def connection_state(self) -> ConnectionState:
        return self._state


async def test_ingest_health_tracks_freshness_after_ingest():
    settings = Settings(
        ingest_transport="noop", ingest_staleness_threshold_seconds=None
    )
    svc = StreamIngestService(
        _FakeConsumer(ConnectionState.CONNECTED), LSMStore(100, 4), settings
    )

    before = await svc.health_check()
    assert before.connection_state == "connected"
    assert before.last_batch_at is None
    assert before.rows_ingested_total == 0

    await svc.ingest_batch(make_batch([(1, "a", "v1", "upsert")]))
    after = await svc.health_check()
    assert after.rows_ingested_total == 1
    assert after.last_batch_at is not None
    assert after.seconds_since_last_batch is not None


async def test_disconnect_watchdog_invokes_shutdown_hook():
    triggered = asyncio.Event()
    settings = Settings(ingest_transport="noop", ingest_max_disconnect_seconds=0.2)
    svc = StreamIngestService(
        _FakeConsumer(ConnectionState.DOWN),
        LSMStore(100, 4),
        settings,
        shutdown_hook=triggered.set,
    )
    async with svc:
        await asyncio.wait_for(triggered.wait(), timeout=3.0)
    assert triggered.is_set()


def test_solace_consumer_ignores_listener_events_after_close():
    consumer = SolaceBatchConsumer(Settings())
    consumer.close()
    # A late SDK callback must not flip a closed consumer back to CONNECTED.
    _StateListener(consumer).on_reconnected(cast(ServiceEvent, None))
    assert consumer.connection_state() == ConnectionState.DOWN


def test_solace_consumer_deliveries_after_close_ends_without_connecting():
    consumer = SolaceBatchConsumer(Settings())
    consumer.close()
    assert list(consumer.deliveries()) == []  # no receiver started, no broker needed


async def test_watchdog_requests_shutdown_when_broker_unreachable(
    solace_container, solace_names, caplog
):
    triggered = asyncio.Event()
    settings = Settings(
        **solace_overrides(solace_container, solace_names),
        ingest_max_disconnect_seconds=1.0,
    )
    async with SolaceBatchConsumer(settings) as consumer:
        svc = StreamIngestService(
            consumer, LSMStore(100, 4), settings, shutdown_hook=triggered.set
        )
        async with svc:
            # Precondition: connected before the partition, so only the pause
            # (not a slow initial connect) can trip the 1 s watchdog.
            await eventually(
                lambda: consumer.connection_state() == ConnectionState.CONNECTED,
                what="consumer connected before partition",
            )
            assert not triggered.is_set()
            with caplog.at_level(logging.CRITICAL, logger="services.stream_ingest"):
                with broker_paused(solace_container):
                    # SDK keepalive needs ~30-35 s to notice a frozen broker.
                    await asyncio.wait_for(triggered.wait(), timeout=120.0)
    assert "ingest transport not connected" in caplog.text


async def test_repeated_bind_denial_requests_shutdown(
    solace_container, solace_names, semp, caplog
):
    # The session is CONNECTED (so the watchdog stays quiet) but every receiver
    # start is denied: the consecutive-failure path must request shutdown.
    # Real backoff: 1 + 2 + 4 + 8 s (+ up to 25% jitter) before failure 5.
    user, password = create_endpoint_denied_user(semp, solace_names)
    triggered = threading.Event()
    settings = Settings(
        **{
            **solace_overrides(solace_container, solace_names),
            "solace_username": user,
            "solace_password": SecretStr(password),
        },
        ingest_max_disconnect_seconds=60.0,
    )
    async with SolaceBatchConsumer(settings) as consumer:
        svc = StreamIngestService(
            consumer, LSMStore(100, 4), settings, shutdown_hook=triggered.set
        )
        with caplog.at_level(logging.CRITICAL, logger="services.stream_ingest"):
            async with svc:
                assert await asyncio.to_thread(triggered.wait, 60.0)
    # Pin the cause: the consecutive-failure path, not the disconnect watchdog.
    assert "consecutive failures; requesting shutdown" in caplog.text
    assert "ingest transport not connected" not in caplog.text
