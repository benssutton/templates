import pyarrow as pa
from solace.messaging.messaging_service import MessagingService
from solace.messaging.publisher.persistent_message_publisher import (
    PersistentMessagePublisher,
)
from solace.messaging.resources.topic import Topic

from tests.publishers.batches import serialize_batch

_ACK_TIMEOUT_MS = 15000


class SolacePublisher:
    """Publishes Guaranteed (persistent) messages and waits for the broker's
    ack, so a returned publish_* call means the message is spooled on every
    queue whose subscription matches the topic."""

    def __init__(
        self, host: str, port: int, vpn: str, username: str, password: str, topic: str
    ) -> None:
        self._topic = Topic.of(topic)
        props = {
            "solace.messaging.transport.host": f"tcp://{host}:{port}",
            "solace.messaging.service.vpn-name": vpn,
            "solace.messaging.authentication.scheme.basic.username": username,
            "solace.messaging.authentication.scheme.basic.password": password,
        }
        self._service: MessagingService = (
            MessagingService.builder().from_properties(props).build()
        )
        self._service.connect()
        self._publisher: PersistentMessagePublisher = (
            self._service.create_persistent_message_publisher_builder().build()
        )
        self._publisher.start()

    def publish_batch(self, batch: pa.RecordBatch) -> None:
        self.publish_bytes(serialize_batch(batch))

    def publish_bytes(self, payload: bytes) -> None:
        # The SDK's build() needs bytearray for a binary payload; plain bytes
        # fails with "Failed to create attachment for the message".
        builder = self._service.message_builder()
        message = builder.build(
            bytearray(payload)
        )  # pyright: ignore[reportArgumentType]
        self._publisher.publish_await_acknowledgement(
            message, self._topic, _ACK_TIMEOUT_MS
        )

    def close(self) -> None:
        try:
            self._publisher.terminate()
        finally:
            self._service.disconnect()
