"""Publish ONE persistent SMF message to $SMF_TOPIC: an Arrow IPC stream of
(id, name, value, op) rows, exactly as python-webservice-template's
SolacePublisher does. Run by tests/it/smf_interop.rs with $SMF_PYTHON -I."""

import os

import pyarrow as pa
import pyarrow.ipc as ipc
from solace.messaging.messaging_service import MessagingService
from solace.messaging.resources.topic import Topic

batch = pa.record_batch(
    {
        "id": pa.array([42], pa.int64()),
        "name": pa.array(["smf"], pa.string()),
        "value": pa.array(["v1"], pa.string()),
        "op": pa.array(["upsert"], pa.string()),
    }
)
sink = pa.BufferOutputStream()
with ipc.new_stream(sink, batch.schema) as writer:
    writer.write_batch(batch)

# SMF on the auth-"none" default VPN accepts any basic credentials; Python's
# SolacePublisher sends admin/admin, so this does too.
props = {
    "solace.messaging.transport.host": f"tcp://{os.environ['SMF_HOST']}:{os.environ['SMF_PORT']}",
    "solace.messaging.service.vpn-name": "default",
    "solace.messaging.authentication.scheme.basic.username": "admin",
    "solace.messaging.authentication.scheme.basic.password": "admin",
}
service = MessagingService.builder().from_properties(props).build()
service.connect()
publisher = service.create_persistent_message_publisher_builder().build()
publisher.start()
try:
    # The SDK needs bytearray for a binary attachment (see solace_publisher.py).
    message = service.message_builder().build(bytearray(sink.getvalue().to_pybytes()))
    publisher.publish_await_acknowledgement(message, Topic.of(os.environ["SMF_TOPIC"]), 15000)
    print("published")
finally:
    publisher.terminate()
    service.disconnect()
