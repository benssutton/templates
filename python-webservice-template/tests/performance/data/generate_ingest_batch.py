"""Run once to regenerate the k6 Arrow fixtures: python generate_ingest_batch.py"""

from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as pa_ipc

SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("name", pa.string()),
        pa.field("value", pa.string()),
        pa.field("op", pa.string()),
    ]
)

# Mixed upsert/delete so the LSM stays exercised under load without growing unboundedly
LOAD_ROWS = [
    (1, "alpha", "v1", "upsert"),
    (2, "beta", "v1", "upsert"),
    (3, "gamma", "v1", "upsert"),
    (1, "alpha", "v2", "upsert"),  # newest-wins for id=1
    (2, "beta", "v1", "delete"),  # tombstone for id=2
]

# contract.js expects exactly [{-3, contract, v2}, {-1, contract, v1}] back from
# /data/cache. Negative ids sort before every id the demo publishers write.
CONTRACT_ROWS = [
    (-3, "contract", "v1", "upsert"),
    (-2, "contract", "v1", "upsert"),
    (-1, "contract", "v1", "upsert"),
    (-3, "contract", "v2", "upsert"),
    (-2, "contract", "v1", "delete"),
]


def write(rows: list[tuple[int, str, str, str]], name: str) -> None:
    batch = pa.record_batch(
        {
            "id": pa.array([r[0] for r in rows], pa.int64()),
            "name": pa.array([r[1] for r in rows], pa.string()),
            "value": pa.array([r[2] for r in rows], pa.string()),
            "op": pa.array([r[3] for r in rows], pa.string()),
        },
        schema=SCHEMA,
    )
    out = Path(__file__).parent / name
    with out.open("wb") as f, pa_ipc.new_stream(f, SCHEMA) as writer:
        writer.write_batch(batch)
    print(f"Written {out} ({out.stat().st_size} bytes)")


write(LOAD_ROWS, "ingest_batch.ipc")
write(CONTRACT_ROWS, "contract_batch.ipc")
