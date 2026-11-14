import asyncio
import logging
import queue
import threading
from typing import Iterator

import pyarrow as pa
import pyarrow.ipc as pa_ipc
from solace.messaging.messaging_service import (
    MessagingService,
    ReconnectionAttemptListener,
    ReconnectionListener,
    ServiceEvent,
    ServiceInterruptionListener,
)
from solace.messaging.receiver.direct_message_receiver import DirectMessageReceiver
from solace.messaging.receiver.inbound_message import InboundMessage
from solace.messaging.receiver.message_receiver import MessageHandler
from solace.messaging.resources.topic_subscription import TopicSubscription

from core.retry import connect_with_backoff
from ingestion.base import ConnectionState
from settings import Settings

log = logging.getLogger(__name__)


class _BatchHandler(MessageHandler):
    def __init__(self, q: queue.Queue) -> None:
        self._queue = q

    def on_message(self, message: InboundMessage) -> None:
        payload = message.get_payload_as_bytes()
        try:
            reader = pa_ipc.open_stream(pa.BufferReader(payload))
            for batch in reader:
                self._queue.put(batch)
        except Exception:
            log.warning("Solace: malformed IPC message dropped", exc_info=True)


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


class SolaceBatchConsumer:
    """Async context manager for connection lifecycle; BatchConsumer for ingest."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._service: MessagingService | None = None
        self._receiver: DirectMessageReceiver | None = None
        self._queue: queue.Queue[pa.RecordBatch | None] = queue.Queue()
        self._state = ConnectionState.DOWN
        self._state_lock = threading.Lock()

    def _set_state(self, state: ConnectionState) -> None:
        with self._state_lock:
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
        props = {
            "solace.messaging.transport.host": f"tcp://{self._settings.solace_host}:{self._settings.solace_port}",
            "solace.messaging.service.vpn-name": self._settings.solace_vpn,
            "solace.messaging.authentication.scheme.basic.username": self._settings.solace_username,
            "solace.messaging.authentication.scheme.basic.password": self._settings.solace_password.get_secret_value(),
        }
        svc = MessagingService.builder().from_properties(props).build()
        svc.connect()  # raises on failure; a fresh service is built per attempt
        listener = _StateListener(self)
        svc.add_reconnection_listener(listener)
        svc.add_reconnection_attempt_listener(listener)
        svc.add_service_interruption_listener(listener)
        self._set_state(ConnectionState.CONNECTED)
        return svc

    async def __aexit__(self, *_: object) -> None:
        await asyncio.to_thread(self.close)

    def batches(self) -> Iterator[pa.RecordBatch]:
        assert self._service is not None, "batches() called before __aenter__"
        # Not chained: the SDK annotates with_subscriptions() as returning the base
        # MessageReceiverBuilder (no build()); it mutates and returns the same builder.
        builder = self._service.create_direct_message_receiver_builder()
        builder.with_subscriptions([TopicSubscription.of(self._settings.solace_topic)])
        receiver = builder.build()
        self._receiver = receiver
        receiver.start()
        receiver.receive_async(_BatchHandler(self._queue))
        while True:
            item = self._queue.get()  # blocks until message or None sentinel
            if item is None:
                break
            yield item

    def close(self) -> None:
        self._set_state(ConnectionState.DOWN)
        self._queue.put(None)  # unblocks batches() generator
        if self._receiver is not None:
            self._receiver.terminate()
            self._receiver = None
        if self._service is not None:
            self._service.disconnect()
            self._service = None
