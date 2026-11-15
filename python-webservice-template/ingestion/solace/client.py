import asyncio
import logging
import queue
import threading
from typing import Iterator

import pyarrow as pa
import pyarrow.ipc as pa_ipc
from solace.messaging.config.message_acknowledgement_configuration import Outcome
from solace.messaging.config.missing_resources_creation_configuration import (
    MissingResourcesCreationStrategy,
)
from solace.messaging.config.retry_strategy import RetryStrategy
from solace.messaging.messaging_service import (
    MessagingService,
    ReconnectionAttemptListener,
    ReconnectionListener,
    ServiceEvent,
    ServiceInterruptionListener,
)
from solace.messaging.receiver.inbound_message import InboundMessage
from solace.messaging.receiver.message_receiver import MessageHandler
from solace.messaging.receiver.persistent_message_receiver import (
    PersistentMessageReceiver,
)
from solace.messaging.resources.queue import Queue
from solace.messaging.resources.topic_subscription import TopicSubscription

from core.retry import connect_with_backoff
from ingestion.base import ConnectionState, Delivery
from settings import Settings

log = logging.getLogger(__name__)

# SDK reconnects forever at this interval; the ingest watchdog
# (ingest_max_disconnect_seconds) is the single place that gives up.
_RECONNECT_INTERVAL_MS = 1000


class _DeliveryHandler(MessageHandler):
    """Runs on an SDK thread. Bound to ONE receiver and ONE inbox (captured, not
    read from the consumer) so it never touches state that close() clears."""

    def __init__(
        self,
        receiver: PersistentMessageReceiver,
        inbox: "queue.Queue[Delivery | None]",
    ) -> None:
        self._receiver = receiver
        self._inbox = inbox

    def on_message(self, message: InboundMessage) -> None:
        receiver = self._receiver
        try:
            payload = message.get_payload_as_bytes() or b""
            batches = list(pa_ipc.open_stream(pa.BufferReader(payload)))
        except Exception:
            # Poison message: it can never decode, so never redeliver it.
            log.warning("Solace: malformed IPC message rejected", exc_info=True)
            try:
                receiver.settle(message, Outcome.REJECTED)
            except Exception:
                # Flow already terminated (closing): broker redelivers on next bind.
                log.debug("Solace: could not reject malformed message", exc_info=True)
            return
        self._inbox.put(
            Delivery(
                batches=batches,
                accept=lambda: receiver.ack(message),
                fail=lambda: receiver.settle(message, Outcome.FAILED),
                reject=lambda: receiver.settle(message, Outcome.REJECTED),
                redelivered=message.is_redelivered(),
            )
        )


class _StateListener(
    ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener
):
    """Bridges Solace SDK lifecycle callbacks (fired on SDK threads) to the
    consumer's connection state. The event argument is unused."""

    def __init__(self, consumer: "SolaceBatchConsumer") -> None:
        self._consumer = consumer

    def on_reconnected(self, event: ServiceEvent) -> None:
        self._consumer._set_state(ConnectionState.CONNECTED)

    def on_reconnecting(self, event: ServiceEvent) -> None:
        self._consumer._set_state(ConnectionState.RECONNECTING)

    def on_service_interrupted(self, event: ServiceEvent) -> None:
        self._consumer._set_state(ConnectionState.DOWN)


def _terminate(receiver: PersistentMessageReceiver | None) -> None:
    """Best effort and safe to repeat. grace_period=0 drops in-flight deliveries
    without settling them, which is safe: unsettled messages stay on the durable
    queue and the broker redelivers them on the next bind (at-least-once)."""
    if receiver is not None:
        try:
            receiver.terminate(grace_period=0)
        except Exception:
            # Already terminated, or the connection is gone.
            log.debug("Solace: receiver terminate failed", exc_info=True)


class SolaceBatchConsumer:
    """Solace Guaranteed messaging: binds a durable exclusive queue (created on
    start, with the topic subscription attached) and yields one Delivery per
    message, settled by StreamIngestService after the LSM write.

    Threading: the MessagingService lives from __aenter__ to close(). Each
    deliveries() call (ingest thread) owns one receiver + one inbox queue and
    tears the receiver down in `finally`, so a retry after a failure can re-bind
    the exclusive queue and never sees stale deliveries. close() (any thread)
    sets `_closed`, wakes the blocked deliveries() via the inbox sentinel and
    terminates whatever receiver is currently published. Both sides tear down
    best-effort, so a race at most terminates the same resource twice.

    Recovery limits: after a terminal service interruption (SDK reconnect gave
    up) or a receiver-level flow termination, deliveries() stays blocked rather
    than raising; recovery then relies on the ingest disconnect watchdog
    (ingest_max_disconnect_seconds; with None it blocks until close())."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        # Deterministic per-replica client name (one queue per replica -> one name).
        self.client_name = settings.solace_queue.replace("/", "_")
        self._service: MessagingService | None = None
        self._closed = threading.Event()
        # Guards the current deliveries() session (receiver + inbox) shared with close().
        self._lock = threading.Lock()
        self._receiver: PersistentMessageReceiver | None = None
        self._inbox: queue.Queue[Delivery | None] | None = None
        self._state = ConnectionState.DOWN
        self._state_lock = threading.Lock()

    def _set_state(self, state: ConnectionState) -> None:
        with self._state_lock:
            if self._closed.is_set():
                return  # late SDK event after close(): stay DOWN
            self._state = state

    def connection_state(self) -> ConnectionState:
        with self._state_lock:
            return self._state

    async def __aenter__(self) -> "SolaceBatchConsumer":
        # The broker's SMF listener accepts TCP (and its health-check endpoint
        # can report active) before the message VPN is serving clients; connect()
        # then fails with 'Not ready' / SERVICE_UNAVAILABLE. Retry with the same
        # bounded exponential backoff as the other startup dependencies.
        async def _attempt() -> MessagingService:
            return await asyncio.to_thread(self._connect)

        self._service = await connect_with_backoff(
            _attempt,
            label="Solace",
            max_attempts=self._settings.connect_max_attempts,
            base_delay=self._settings.connect_base_delay,
            max_delay=self._settings.connect_max_delay,
        )
        return self

    def _connect(self) -> MessagingService:
        s = self._settings
        props = {
            "solace.messaging.transport.host": f"tcp://{s.solace_host}:{s.solace_port}",
            "solace.messaging.service.vpn-name": s.solace_vpn,
            "solace.messaging.authentication.scheme.basic.username": s.solace_username,
            "solace.messaging.authentication.scheme.basic.password": s.solace_password.get_secret_value(),
        }
        svc = (
            MessagingService.builder()
            .from_properties(props)
            .with_reconnection_retry_strategy(
                RetryStrategy.forever_retry(_RECONNECT_INTERVAL_MS)
            )
            .build(self.client_name)
        )
        try:
            svc.connect()  # raises on failure; a fresh service is built per attempt
            listener = _StateListener(self)
            svc.add_reconnection_listener(listener)
            svc.add_reconnection_attempt_listener(listener)
            svc.add_service_interruption_listener(listener)
        except Exception:
            # Don't leak this attempt's service (and its SDK threads/socket).
            try:
                svc.disconnect()
            except Exception:
                log.debug("Solace: disconnect after failed connect", exc_info=True)
            raise
        self._set_state(ConnectionState.CONNECTED)
        return svc

    async def __aexit__(self, *_: object) -> None:
        await asyncio.to_thread(self.close)

    def deliveries(self) -> Iterator[Delivery]:
        service = self._service  # local: close() clears the attribute
        if self._closed.is_set():
            return
        if service is None:
            raise RuntimeError("deliveries() called before __aenter__")
        s = self._settings
        receiver: PersistentMessageReceiver | None = None
        try:
            # Built inside the try: if close() races construction (the service is
            # disconnected under us), the except below turns it into a clean stop.
            # Not chained: the SDK annotates with_subscriptions() as returning the
            # base MessageReceiverBuilder (no build()); every call mutates it.
            builder = service.create_persistent_message_receiver_builder()
            builder.with_missing_resources_creation_strategy(
                MissingResourcesCreationStrategy.CREATE_ON_START
            )
            builder.with_message_client_acknowledgement()  # settle after the write
            builder.with_required_message_outcome_support(
                Outcome.FAILED, Outcome.REJECTED
            )
            builder.with_subscriptions([TopicSubscription.of(s.solace_topic)])
            receiver = builder.build(Queue.durable_exclusive_queue(s.solace_queue))
            # Fresh per call; unbounded in type but effectively bounded: the broker
            # stops delivering once the flow's unacked window is full.
            inbox: queue.Queue[Delivery | None] = queue.Queue()
            # Publish before start so close() can terminate us even mid-start().
            with self._lock:
                self._receiver, self._inbox = receiver, inbox
            if self._closed.is_set():  # close() ran before we published
                return
            receiver.start()  # raises if the broker denies queue creation/bind
            if self._closed.is_set():  # close() ran while we were starting
                return
            receiver.receive_async(_DeliveryHandler(receiver, inbox))
            while True:
                item = inbox.get()  # blocks until a delivery or the None sentinel
                # The sentinel queues BEHIND buffered deliveries; don't hand those
                # out once closed (their flow is terminated; the broker redelivers).
                if item is None or self._closed.is_set():
                    return
                yield item
        except Exception:
            if self._closed.is_set():
                return  # close() tore the session down under us: a clean stop
            raise
        finally:
            if receiver is not None:
                with self._lock:
                    if self._receiver is receiver:
                        self._receiver = self._inbox = None
                _terminate(receiver)  # on ANY exit, so a retry can re-bind the queue

    def close(self) -> None:
        self._closed.set()
        with self._state_lock:
            self._state = ConnectionState.DOWN
        with self._lock:
            receiver, inbox = self._receiver, self._inbox
        if inbox is not None:
            inbox.put(None)  # unblocks deliveries()
        # deliveries()'s finally also terminates; doing it here too unblocks it if
        # it is stuck inside the SDK (start) and covers a shutdown timeout.
        _terminate(receiver)
        service, self._service = self._service, None
        if service is not None:
            try:
                service.disconnect()
            except Exception:
                log.debug("Solace: disconnect failed", exc_info=True)
