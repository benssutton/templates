import asyncio
import logging
import queue
import threading
from collections.abc import Iterator
from enum import Enum

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

from core.retry import connect_with_backoff, connect_with_backoff_sync
from ingestion.base import ConnectionState, Delivery, OnBound
from settings import Settings

log = logging.getLogger(__name__)

# SDK reconnects forever at this interval. If it still reports a terminal
# interruption (e.g. the broker refuses the login), deliveries() raises and the
# next call reconnects; the ingest watchdog (ingest_max_disconnect_seconds) and
# the consecutive-failure limit are the places that give up.
_RECONNECT_INTERVAL_MS = 1000


class SolaceServiceInterrupted(ConnectionError):
    """The SDK gave up on the MessagingService (terminal interruption). Raised
    from deliveries() so the ingest loop counts a failure and backs off; the
    next deliveries() call reconnects with a fresh service."""


class _Signal(Enum):
    """Identity sentinels placed on a deliveries() inbox to wake it."""

    CLOSED = "closed"
    INTERRUPTED = "interrupted"


_InboxItem = Delivery | _Signal


class _Session:
    """One MessagingService, and whether the SDK has terminally given up on it.
    `interrupted` is set by that service's own listener whether or not the
    session is still current, so deliveries() can re-check it without races."""

    def __init__(self, service: MessagingService) -> None:
        self.service = service
        self.interrupted = threading.Event()


class _DeliveryHandler(MessageHandler):
    """Runs on an SDK thread. Bound to ONE receiver and ONE inbox (captured, not
    read from the consumer) so it never touches state that close() clears."""

    def __init__(
        self,
        receiver: PersistentMessageReceiver,
        inbox: "queue.Queue[_InboxItem]",
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
    consumer's connection state. Bound to ONE session: events from a torn-down
    service can't overwrite a newer session's state. The event is unused."""

    def __init__(self, consumer: "SolaceBatchConsumer", session: _Session) -> None:
        self._consumer = consumer
        self._session = session

    def on_reconnected(self, service_event: ServiceEvent) -> None:
        self._consumer._set_state(ConnectionState.CONNECTED, self._session)

    def on_reconnecting(self, event: ServiceEvent) -> None:
        self._consumer._set_state(ConnectionState.RECONNECTING, self._session)

    def on_service_interrupted(self, event: ServiceEvent) -> None:
        # Terminal: this service will never recover (the SDK stopped retrying).
        self._session.interrupted.set()
        self._consumer._on_interrupted(self._session)


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


def _disconnect(session: _Session | None) -> None:
    """Best effort and safe to repeat."""
    if session is not None:
        try:
            session.service.disconnect()
        except Exception:
            log.debug("Solace: disconnect failed", exc_info=True)


class SolaceBatchConsumer:
    """Solace Guaranteed messaging: binds a durable exclusive queue (created on
    start, with the topic subscription attached) and yields one Delivery per
    message, settled by StreamIngestService after the LSM write.

    Threading: __aenter__ connects a MessagingService (with backoff; startup
    fails if the broker is unreachable). Each deliveries() call (ingest thread)
    owns one receiver + one inbox queue and tears the receiver down in
    `finally`, so a retry after a failure can re-bind the exclusive queue and
    never sees stale deliveries. close() (any thread) sets `_closed`, wakes the
    blocked deliveries() via the inbox sentinel and terminates whatever receiver
    and service are currently published. Both sides tear down best-effort, so a
    race at most terminates the same resource twice.

    Recovery: network blips are handled by the SDK (forever retry; the
    persistent flow re-binds). A terminal service interruption (the SDK gave up,
    e.g. the broker refused the re-login) wakes deliveries() with a sentinel and
    it raises SolaceServiceInterrupted; the ingest loop counts a failure and
    backs off, and the next deliveries() call disconnects the dead service and
    connects a fresh one (same backoff policy as startup). A receiver-level flow
    termination on a live service is not signalled: deliveries() stays blocked,
    and the disconnect watchdog does not see it either (the service is CONNECTED).
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        # Deterministic per-replica client name (one queue per replica -> one name).
        self.client_name = settings.solace_queue.replace("/", "_")
        self._closed = threading.Event()
        # Guards everything shared with close() and SDK threads: the current
        # session (service), the current deliveries() receiver + inbox, and state.
        self._lock = threading.Lock()
        self._session: _Session | None = None
        self._receiver: PersistentMessageReceiver | None = None
        self._inbox: queue.Queue[_InboxItem] | None = None
        self._state = ConnectionState.DOWN

    def _set_state(self, state: ConnectionState, session: _Session) -> None:
        with self._lock:
            # Late event from a replaced service, or after close(): close() clears
            # _session and _publish_session() refuses once closed, so the identity
            # check covers both.
            if session is not self._session:
                return
            self._state = state

    def _on_interrupted(self, session: _Session) -> None:
        with self._lock:
            if session is not self._session:  # replaced, or closed (see _set_state)
                return
            self._state = ConnectionState.DOWN
            inbox = self._inbox
        if inbox is not None:
            inbox.put(_Signal.INTERRUPTED)  # wakes deliveries(), which raises

    def _publish_session(self, session: _Session) -> bool:
        """Make `session` current. False (and disconnected) if close() won the race."""
        with self._lock:
            if not self._closed.is_set():
                self._session = session
                # Interrupted before it became current: its listener's event was
                # dropped by the identity check, so don't report it as healthy.
                self._state = (
                    ConnectionState.DOWN
                    if session.interrupted.is_set()
                    else ConnectionState.CONNECTED
                )
                return True
        _disconnect(session)
        return False

    def connection_state(self) -> ConnectionState:
        with self._lock:
            return self._state

    async def __aenter__(self) -> "SolaceBatchConsumer":
        # The broker's SMF listener accepts TCP (and its health-check endpoint
        # can report active) before the message VPN is serving clients; connect()
        # then fails with 'Not ready' / SERVICE_UNAVAILABLE. Retry with the same
        # bounded exponential backoff as the other startup dependencies.
        async def _attempt() -> _Session:
            return await asyncio.to_thread(self._connect)

        session = await connect_with_backoff(
            _attempt,
            label="Solace",
            max_attempts=self._settings.connect_max_attempts,
            base_delay=self._settings.connect_base_delay,
            max_delay=self._settings.connect_max_delay,
        )
        self._publish_session(session)
        return self

    def _connect(self) -> _Session:
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
        session = _Session(svc)
        try:
            svc.connect()  # raises on failure; a fresh service is built per attempt
            listener = _StateListener(self, session)
            svc.add_reconnection_listener(listener)
            svc.add_reconnection_attempt_listener(listener)
            svc.add_service_interruption_listener(listener)
        except Exception:
            # Don't leak this attempt's service (and its SDK threads/socket).
            _disconnect(session)
            raise
        return session

    def _reconnect(self, dead: _Session) -> _Session | None:
        """Replace a terminally interrupted session. None if close() ran; raises
        the last connect error once the backoff budget is spent."""
        _disconnect(dead)
        if self._closed.is_set():
            return None
        log.warning("Solace: service was terminally interrupted; reconnecting")
        s = self._settings

        def _attempt() -> _Session:
            if self._closed.is_set():  # e.g. close() during the previous attempt
                raise RuntimeError("Solace: consumer closed")
            return self._connect()

        session = connect_with_backoff_sync(
            _attempt,
            label="Solace reconnect",
            max_attempts=s.connect_max_attempts,
            base_delay=s.connect_base_delay,
            max_delay=s.connect_max_delay,
            wait=self._closed.wait,  # close() cuts the backoff short
        )
        return session if self._publish_session(session) else None

    async def __aexit__(self, *_: object) -> None:
        await asyncio.to_thread(self.close)

    def deliveries(self, on_bound: OnBound | None = None) -> Iterator[Delivery]:
        with self._lock:
            session = self._session  # local: close() clears the attribute
        if self._closed.is_set():
            return
        if session is None:
            raise RuntimeError("deliveries() called before __aenter__")
        s = self._settings
        receiver: PersistentMessageReceiver | None = None
        try:
            if session.interrupted.is_set():
                reconnected = self._reconnect(session)
                if reconnected is None:
                    return  # closed while reconnecting
                session = reconnected
            # Built inside the try: if close() races construction (the service is
            # disconnected under us), the except below turns it into a clean stop.
            # Not chained: the SDK annotates with_subscriptions() as returning the
            # base MessageReceiverBuilder (no build()); every call mutates it.
            builder = session.service.create_persistent_message_receiver_builder()
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
            inbox: queue.Queue[_InboxItem] = queue.Queue()
            # Publish before start so close() can terminate us even mid-start(),
            # and so an interruption from here on wakes us via the inbox.
            with self._lock:
                self._receiver, self._inbox = receiver, inbox
            if self._closed.is_set():  # close() ran before we published
                return
            if session.interrupted.is_set():  # interrupted before we published
                raise SolaceServiceInterrupted("Solace: service terminally interrupted")
            receiver.start()  # raises if the broker denies queue creation/bind
            if self._closed.is_set():  # close() ran while we were starting
                return
            receiver.receive_async(_DeliveryHandler(receiver, inbox))
            if on_bound is not None:
                on_bound()  # bound: the transport is healthy
            while True:
                item = inbox.get()  # blocks until a delivery or a sentinel
                # Sentinels queue BEHIND buffered deliveries; don't hand those out
                # once closed (their flow is terminated; the broker redelivers).
                if item is _Signal.CLOSED or self._closed.is_set():
                    return
                if item is _Signal.INTERRUPTED:
                    raise SolaceServiceInterrupted(
                        "Solace: service terminally interrupted"
                    )
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
        with self._lock:
            self._state = ConnectionState.DOWN
            receiver, inbox, session = self._receiver, self._inbox, self._session
            self._session = None
        if inbox is not None:
            inbox.put(_Signal.CLOSED)  # unblocks deliveries()
        # deliveries()'s finally also terminates; doing it here too unblocks it if
        # it is stuck inside the SDK (start) and covers a shutdown timeout.
        _terminate(receiver)
        _disconnect(session)
