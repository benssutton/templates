"""SolaceBatchConsumer against a real broker: queue self-provisioning,
settlement outcomes, poison-message handling, reconnect, permission denial and
lifecycle (close / teardown). Broker-side facts are asserted via SEMP."""

import asyncio
import logging
import queue
import threading
import time
from collections.abc import Generator
from contextlib import asynccontextmanager, suppress
from typing import cast

import pytest
from pydantic import SecretStr
from solace.messaging.errors.pubsubplus_client_error import PubSubPlusClientError
from solace.messaging.messaging_service import ServiceEvent

from ingestion.base import ConnectionState, Delivery
from ingestion.solace.client import (
    SolaceBatchConsumer,
    SolaceServiceInterrupted,
    _Session,
    _StateListener,
)
from settings import Settings
from tests.publishers.batches import make_batch
from tests.publishers.solace_publisher import SolacePublisher
from tests.support.settings import solace_overrides
from tests.support.solace import (
    broker_paused,
    create_endpoint_denied_user,
    wait_for_queue,
)
from tests.support.wait import eventually


def _settings(solace_container, names, **overrides) -> Settings:
    return Settings(**{**solace_overrides(solace_container, names), **overrides})


@asynccontextmanager
async def running_consumer(settings: Settings):
    """Drain deliveries() on a thread, as the ingest loop would."""
    received: queue.Queue[Delivery] = queue.Queue()
    drain_error: list[BaseException] = []
    async with SolaceBatchConsumer(settings) as consumer:

        def _drain() -> None:
            try:
                for delivery in consumer.deliveries():
                    received.put(delivery)
            except BaseException as exc:  # surfaced in finally, not as queue.Empty
                drain_error.append(exc)

        thread = threading.Thread(target=_drain, daemon=True)
        thread.start()
        try:
            yield consumer, received
        finally:
            await asyncio.to_thread(consumer.close)
            await asyncio.to_thread(thread.join, 10)
        # Reached only if the body passed: a failing body keeps its own error.
        if drain_error:
            raise drain_error[0]


async def _next(received: queue.Queue[Delivery], timeout: float = 15.0) -> Delivery:
    return await asyncio.to_thread(received.get, True, timeout)


async def _close_and_reap(consumer: SolaceBatchConsumer, task: asyncio.Task) -> None:
    """Close first: it unblocks a worker thread still parked in next(gen), so
    the task can finish instead of hanging the session. The task's own outcome
    (e.g. StopIteration once closed) is irrelevant here: the test's assertions
    already decided pass/fail."""
    await asyncio.to_thread(consumer.close)
    # CancelledError too: a timed-out wait_for in the test has cancelled it.
    with suppress(Exception, asyncio.CancelledError):
        await asyncio.wait_for(task, 20)


ONE_ROW = [(1, "a", "v1", "upsert")]


async def test_start_provisions_durable_queue_with_topic_subscription(
    solace_container, solace_names, semp
):
    async with running_consumer(_settings(solace_container, solace_names)):
        await wait_for_queue(semp, solace_names.queue)
        assert semp.queue_subscriptions(solace_names.queue) == [solace_names.topic]


async def test_accept_removes_message_from_queue(
    solace_container, solace_names, semp, solace_publisher
):
    async with running_consumer(_settings(solace_container, solace_names)) as (
        _,
        received,
    ):
        await wait_for_queue(semp, solace_names.queue)
        solace_publisher.publish_batch(make_batch(ONE_ROW))
        delivery = await _next(received)
        assert delivery.batches[0].num_rows == 1
        assert delivery.redelivered is False
        # delivered but unacked: still spooled
        assert semp.queue_message_count(solace_names.queue) == 1
        delivery.accept()
        await eventually(
            lambda: semp.queue_message_count(solace_names.queue) == 0,
            what="queue drained",
        )


async def test_fail_redelivers_message(
    solace_container, solace_names, semp, solace_publisher
):
    async with running_consumer(_settings(solace_container, solace_names)) as (
        _,
        received,
    ):
        await wait_for_queue(semp, solace_names.queue)
        solace_publisher.publish_batch(make_batch(ONE_ROW))
        first = await _next(received)
        first.fail()
        again = await _next(received)
        assert again.redelivered is True
        assert again.batches[0].column("id").to_pylist() == [1]
        again.accept()


async def test_reject_removes_message_without_redelivery(
    solace_container, solace_names, semp, solace_publisher
):
    async with running_consumer(_settings(solace_container, solace_names)) as (
        _,
        received,
    ):
        await wait_for_queue(semp, solace_names.queue)
        solace_publisher.publish_batch(make_batch(ONE_ROW))
        (await _next(received)).reject()
        await eventually(
            lambda: semp.queue_message_count(solace_names.queue) == 0,
            what="queue drained",
        )
        # Belt-and-braces negative check: spooled == 0 already proves the broker
        # will not redeliver; this just confirms nothing late reached the consumer.
        await asyncio.sleep(2)
        assert received.empty()


async def test_malformed_payload_is_rejected_by_the_consumer(
    solace_container, solace_names, semp, solace_publisher, caplog
):
    async with running_consumer(_settings(solace_container, solace_names)) as (
        _,
        received,
    ):
        await wait_for_queue(semp, solace_names.queue)
        with caplog.at_level(logging.WARNING, logger="ingestion.solace.client"):
            solace_publisher.publish_bytes(b"definitely not arrow ipc")
            await eventually(
                lambda: semp.queue_message_count(solace_names.queue) == 0,
                what="poison rejected",
            )
        assert received.empty()  # never handed to the ingest loop
        assert "malformed IPC message rejected" in caplog.text


async def test_reconnects_and_resumes_after_network_partition(
    solace_container, solace_names, semp
):
    async with running_consumer(_settings(solace_container, solace_names)) as (
        consumer,
        received,
    ):
        await wait_for_queue(semp, solace_names.queue)
        # Pauses the session-shared broker container: must not run concurrently
        # with any other test using the broker (no xdist across broker tests).
        with broker_paused(solace_container):
            # Keepalives go unanswered (~3 x 3 s); forever-retry means the SDK
            # reports RECONNECTING, never a terminal interruption.
            await eventually(
                lambda: consumer.connection_state() == ConnectionState.RECONNECTING,
                timeout=45,
                what="RECONNECTING during partition",
            )
        await eventually(
            lambda: consumer.connection_state() == ConnectionState.CONNECTED,
            timeout=45,
            what="CONNECTED after partition heals",
        )
        # The persistent flow re-binds automatically: new messages still arrive.
        # Fresh publisher: the fixture's connection may not survive the pause.
        publisher = SolacePublisher(
            host=solace_container.get_container_host_ip(),
            port=int(solace_container.get_exposed_port(55555)),
            vpn="default",
            username="admin",
            password="admin",
            topic=solace_names.topic,
        )
        try:
            publisher.publish_batch(make_batch([(7, "after", "v1", "upsert")]))
            delivery = await _next(received, timeout=30)
            assert delivery.batches[0].column("id").to_pylist() == [7]
            delivery.accept()
        finally:
            publisher.close()


async def test_terminal_interruption_raises_and_next_call_reconnects(
    solace_container, solace_names, semp, solace_publisher
):
    """Drives the SDK's terminal-interruption callback directly. A broker-side
    trigger isn't reliable: with forever-retry the SDK keeps RECONNECTING when
    the client username or the message VPN is disabled (both tried via SEMP)."""
    consumer = SolaceBatchConsumer(
        _settings(solace_container, solace_names, connect_base_delay=0.1)
    )
    async with consumer:
        gen = consumer.deliveries()
        first_run = asyncio.create_task(asyncio.to_thread(next, gen))
        second_run: asyncio.Task | None = None
        try:
            await wait_for_queue(semp, solace_names.queue)
            await eventually(
                lambda: semp.queue_bind_count(solace_names.queue) == 1,
                what="flow bound",
            )
            dead = consumer._session
            assert dead is not None
            _interrupt(consumer, dead)
            with pytest.raises(SolaceServiceInterrupted):
                await asyncio.wait_for(first_run, 10)
            assert consumer.connection_state() == ConnectionState.DOWN
            await eventually(
                lambda: semp.queue_bind_count(solace_names.queue) == 0,
                what="receiver torn down after the raise",
            )

            second = consumer.deliveries()  # hold a reference: GC would close it
            second_run = asyncio.create_task(asyncio.to_thread(next, second))
            await eventually(
                lambda: semp.queue_bind_count(solace_names.queue) == 1,
                what="re-bound on a fresh service",
            )
            assert consumer._session is not dead
            assert consumer.connection_state() == ConnectionState.CONNECTED
            # The dead service was disconnected, not leaked alongside the new one.
            await eventually(
                lambda: semp.client_count(consumer.client_name) == 1,
                what="exactly one client connected",
            )
            solace_publisher.publish_batch(make_batch([(9, "again", "v1", "upsert")]))
            delivery = await asyncio.wait_for(second_run, 20)
            assert delivery.batches[0].column("id").to_pylist() == [9]
            delivery.accept()
            await asyncio.to_thread(cast(Generator[Delivery, None, None], second).close)
        finally:
            await _close_and_reap(consumer, first_run)
            if second_run is not None:
                await _close_and_reap(consumer, second_run)


def _interrupt(consumer: SolaceBatchConsumer, session: _Session) -> None:
    """What the SDK calls once it has given up reconnecting this service."""
    _StateListener(consumer, session).on_service_interrupted(cast(ServiceEvent, None))


async def test_interruption_between_calls_reconnects_on_entry_and_reports_bound(
    solace_container, solace_names, semp, solace_publisher
):
    """Interrupted while no deliveries() is running (e.g. during the ingest
    loop's backoff): the next call replaces the dead service before binding,
    then signals on_bound."""
    consumer = SolaceBatchConsumer(
        _settings(solace_container, solace_names, connect_base_delay=0.1)
    )
    async with consumer:
        dead = consumer._session
        assert dead is not None
        _interrupt(consumer, dead)
        assert consumer.connection_state() == ConnectionState.DOWN
        bound = threading.Event()
        gen = consumer.deliveries(on_bound=bound.set)
        run = asyncio.create_task(asyncio.to_thread(next, gen))
        try:
            await wait_for_queue(semp, solace_names.queue)
            assert await asyncio.to_thread(bound.wait, 20), "on_bound not called"
            assert consumer._session is not dead
            assert consumer.connection_state() == ConnectionState.CONNECTED
            solace_publisher.publish_batch(make_batch([(3, "x", "v1", "upsert")]))
            delivery = await asyncio.wait_for(run, 20)
            assert delivery.batches[0].column("id").to_pylist() == [3]
            delivery.accept()
            await asyncio.to_thread(cast(Generator[Delivery, None, None], gen).close)
        finally:
            await _close_and_reap(consumer, run)


class _InterruptedBeforeBind(SolaceBatchConsumer):
    """Real consumer whose FIRST reconnected service is interrupted at once:
    the SDK event lands after the reconnect but before deliveries() has
    published its inbox, so only the pre-bind re-check can notice it."""

    def __init__(self, settings: Settings) -> None:
        super().__init__(settings)
        self.armed = True

    def _reconnect(self, dead: _Session) -> _Session | None:
        session = super()._reconnect(dead)
        if session is not None and self.armed:
            self.armed = False
            _interrupt(self, session)
        return session


async def test_interruption_before_bind_raises_then_next_call_reconnects(
    solace_container, solace_names, semp, solace_publisher
):
    consumer = _InterruptedBeforeBind(
        _settings(solace_container, solace_names, connect_base_delay=0.1)
    )
    async with consumer:
        first = consumer._session
        assert first is not None
        _interrupt(consumer, first)  # so the first call reconnects on entry
        bound = threading.Event()
        gen = consumer.deliveries(on_bound=bound.set)
        with pytest.raises(SolaceServiceInterrupted):
            await asyncio.wait_for(asyncio.to_thread(next, gen), 20)
        assert not bound.is_set()  # raised before receiver.start()
        assert not consumer.armed
        assert consumer.connection_state() == ConnectionState.DOWN
        assert not semp.queue_exists(solace_names.queue)  # never bound

        second = consumer.deliveries(on_bound=bound.set)
        run = asyncio.create_task(asyncio.to_thread(next, second))
        try:
            await wait_for_queue(semp, solace_names.queue)
            assert await asyncio.to_thread(bound.wait, 20), "on_bound not called"
            assert consumer.connection_state() == ConnectionState.CONNECTED
            await eventually(
                lambda: semp.client_count(consumer.client_name) == 1,
                what="exactly one client connected",
            )
            solace_publisher.publish_batch(make_batch([(4, "y", "v1", "upsert")]))
            delivery = await asyncio.wait_for(run, 20)
            assert delivery.batches[0].column("id").to_pylist() == [4]
            delivery.accept()
            await asyncio.to_thread(cast(Generator[Delivery, None, None], second).close)
        finally:
            await _close_and_reap(consumer, run)


async def test_bind_fails_when_client_profile_denies_queue_creation(
    solace_container, solace_names, semp
):
    user, password = create_endpoint_denied_user(semp, solace_names)
    settings = _settings(
        solace_container,
        solace_names,
        solace_username=user,
        solace_password=SecretStr(password),
    )
    async with SolaceBatchConsumer(settings) as consumer:
        with pytest.raises(PubSubPlusClientError):
            await asyncio.wait_for(
                asyncio.to_thread(next, iter(consumer.deliveries())), timeout=20
            )
    assert not semp.queue_exists(solace_names.queue)


# ── Lifecycle ──────────────────────────────────────────────────────────────


async def test_close_before_deliveries_returns_promptly_and_never_creates_queue(
    solace_container, solace_names, semp
):
    consumer = SolaceBatchConsumer(_settings(solace_container, solace_names))
    async with consumer:
        pass  # __aexit__ -> close()
    started = time.monotonic()
    items = await asyncio.to_thread(lambda: list(consumer.deliveries()))
    assert items == []
    assert time.monotonic() - started < 5
    assert not semp.queue_exists(solace_names.queue)
    await eventually(
        lambda: semp.client_count(consumer.client_name) == 0,
        what="client disconnected",
    )


async def test_close_while_blocked_ends_generator_and_unbinds_flow(
    solace_container, solace_names, semp
):
    consumer = SolaceBatchConsumer(_settings(solace_container, solace_names))
    await consumer.__aenter__()
    done = threading.Event()
    try:

        def _drain() -> None:
            for _ in consumer.deliveries():
                pass
            done.set()

        thread = threading.Thread(target=_drain, daemon=True)
        thread.start()
        await wait_for_queue(semp, solace_names.queue)
        await eventually(
            lambda: semp.queue_bind_count(solace_names.queue) == 1,
            what="flow bound",
        )
        await eventually(
            lambda: semp.client_count(consumer.client_name) == 1,
            what="client connected",
        )
        assert not done.is_set()
        started = time.monotonic()
        await asyncio.to_thread(consumer.close)
        assert await asyncio.to_thread(done.wait, 10), "deliveries() did not end"
        assert time.monotonic() - started < 10
        await eventually(
            lambda: semp.queue_bind_count(solace_names.queue) == 0,
            what="flow unbound after close",
        )
        await eventually(
            lambda: semp.client_count(consumer.client_name) == 0,
            what="client disconnected",
        )
        assert consumer.connection_state() == ConnectionState.DOWN
        thread.join(5)
    finally:
        consumer.close()


async def test_abandoned_deliveries_unbinds_queue_and_rebind_redelivers(
    solace_container, solace_names, semp, solace_publisher
):
    consumer = SolaceBatchConsumer(_settings(solace_container, solace_names))
    async with consumer:
        gen = consumer.deliveries()
        first_run = asyncio.create_task(asyncio.to_thread(next, gen))
        try:
            await wait_for_queue(semp, solace_names.queue)
            await eventually(
                lambda: semp.queue_bind_count(solace_names.queue) == 1,
                what="flow bound",
            )
            solace_publisher.publish_batch(make_batch([(5, "x", "v1", "upsert")]))
            first = await asyncio.wait_for(first_run, 20)
            assert first.redelivered is False
            # The consuming code fails mid-loop: the generator is closed
            # without the delivery having been settled.
            await asyncio.to_thread(cast(Generator[Delivery, None, None], gen).close)
            await eventually(
                lambda: semp.queue_bind_count(solace_names.queue) == 0,
                what="exclusive queue unbound after generator exit",
            )
            assert semp.queue_message_count(solace_names.queue) == 1

            second = consumer.deliveries()  # hold a reference: GC would close it
            again = await asyncio.wait_for(asyncio.to_thread(next, second), 20)
            assert again.redelivered is True
            assert again.batches[0].column("id").to_pylist() == [5]
            again.accept()
            await eventually(
                lambda: semp.queue_message_count(solace_names.queue) == 0,
                what="acked",
            )
            await asyncio.to_thread(cast(Generator[Delivery, None, None], second).close)
        finally:
            await _close_and_reap(consumer, first_run)


async def test_buffered_messages_are_not_handed_out_after_close(
    solace_container, solace_names, semp, solace_publisher
):
    consumer = SolaceBatchConsumer(_settings(solace_container, solace_names))
    async with consumer:
        gen = consumer.deliveries()
        first_task = asyncio.create_task(asyncio.to_thread(next, gen))
        try:
            await wait_for_queue(semp, solace_names.queue)
            await eventually(
                lambda: semp.queue_bind_count(solace_names.queue) == 1,
                what="flow bound",
            )
            solace_publisher.publish_batch(make_batch(ONE_ROW))
            first = await asyncio.wait_for(first_task, 20)
            assert first.batches[0].column("id").to_pylist() == [1]
            # Not consumed: these land in the consumer's inbox buffer.
            for i in (2, 3, 4):
                solace_publisher.publish_batch(make_batch([(i, "a", "v1", "upsert")]))
            await eventually(
                lambda: semp.queue_message_count(solace_names.queue) == 4,
                what="4 messages spooled",
            )
            # All four are on the flow, unsettled: the yielded-but-unsettled
            # first delivery plus three sitting in the consumer's inbox.
            await eventually(
                lambda: semp.queue_unacked_count(solace_names.queue) == 4,
                what="4 messages delivered to the flow, unacked",
            )
            await asyncio.to_thread(consumer.close)
            assert (
                await asyncio.wait_for(asyncio.to_thread(next, gen, None), 20) is None
            )
            await eventually(
                lambda: semp.queue_bind_count(solace_names.queue) == 0,
                what="flow unbound after close",
            )
            assert semp.queue_unacked_count(solace_names.queue) == 0
            # Nothing was settled or handed out: all four remain on the queue.
            assert semp.queue_message_count(solace_names.queue) == 4
        finally:
            await _close_and_reap(consumer, first_task)
