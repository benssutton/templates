"""Transport-neutral Arrow test data shared by every publisher and test."""

import pyarrow as pa
import pyarrow.ipc as pa_ipc

RECORD_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("name", pa.string()),
        pa.field("value", pa.string()),
        pa.field("op", pa.string()),
    ]
)


def make_batch(rows: list[tuple[int, str, str, str]]) -> pa.RecordBatch:
    """rows: list of (id, name, value, op)."""
    return pa.record_batch(
        {
            "id": pa.array([r[0] for r in rows], pa.int64()),
            "name": pa.array([r[1] for r in rows], pa.string()),
            "value": pa.array([r[2] for r in rows], pa.string()),
            "op": pa.array([r[3] for r in rows], pa.string()),
        }
    )


def serialize_batch(batch: pa.RecordBatch) -> bytes:
    """Encode one batch as an Arrow IPC *stream* (the wire format of every transport)."""
    buf = pa.BufferOutputStream()
    with pa_ipc.new_stream(buf, batch.schema) as writer:
        writer.write_batch(batch)
    return buf.getvalue().to_pybytes()
