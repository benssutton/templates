import asyncio
import logging
import threading
import time

import pyarrow as pa
import pyarrow.flight as pa_flight
import pyarrow.ipc as pa_ipc
import pytest
from httpx import AsyncClient

from settings import Settings
from tests.app_client import lifespan_test_client
from tests.publishers.flight_server import ExampleFlightServer, make_batch

pytestmark = pytest.mark.http_ingest


def _serialize_batch(batch: pa.RecordBatch) -> bytes:
    buf = pa.BufferOutputStream()
    with pa_ipc.new_stream(buf, batch.schema) as writer:
        writer.write_batch(batch)
    return buf.getvalue().to_pybytes()


@pytest.fixture(scope="module")
def empty_flight_server():
    location = pa_flight.Location.for_grpc_tcp("localhost", 0)
    # Empty script: server accepts connections but sends zero batches
    server = ExampleFlightServer(location, [], interval=0.0, loop=False)
    threading.Thread(target=server.serve, daemon=True).start()
    yield server
    server.shutdown()


@pytest.fixture(scope="module")
async def test_client_http(
    postgres_container,
    clickhouse_container,
    test_clickhouse_client,
    redis_container,
    empty_flight_server,
):
    pg_port = int(postgres_container.get_exposed_port(5432))
    ch_port = int(clickhouse_container.get_exposed_port(8123))
    redis_port = int(redis_container.get_exposed_port(6379))

    http_settings = Settings(
        status="testing",
        postgres_url=f"postgresql://{postgres_container.username}:{postgres_container.password}@localhost:{pg_port}/{postgres_container.dbname}",
        clickhouse_host="localhost",
        clickhouse_port=ch_port,
        clickhouse_user=clickhouse_container.username or "default",
        clickhouse_password=clickhouse_container.password or "",
        clickhouse_database="default",
        redis_url=f"redis://localhost:{redis_port}/0",
        ingest_transport="flight",
        flight_host="localhost",
        flight_port=empty_flight_server.port,
        flight_ticket="items",
        lsm_flush_rows=2,
        lsm_compaction_runs=2,
        ingest_max_disconnect_seconds=None,  # empty server by design; watchdog would kill the session
    )

    # Isolated app: own container, own FastMCP, own lifespan — safe to run
    # in the same process as the session-scoped flight test_client.
    async with lifespan_test_client(http_settings) as client:
        yield client


async def _poll_for_value(client: AsyncClient, id_: int, expected_value: str, timeout: float = 10.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = (await client.get("/data/cache?limit=100")).json()
        rows_by_id = {r["id"]: r for r in body["rows"]}
        if id_ in rows_by_id and rows_by_id[id_]["value"] == expected_value:
            return rows_by_id[id_]
        await asyncio.sleep(0.05)
    raise AssertionError(f"id={id_} with value={expected_value!r} never appeared in cache")


async def test_lsm_cache_empty_before_ingest(test_client_http: AsyncClient):
    body = (await test_client_http.get("/data/cache?limit=100")).json()
    assert body["total"] == 0
    assert body["rows"] == []


async def test_post_ingest_upsert_appears_in_cache(test_client_http: AsyncClient):
    batch = make_batch([(100, "http", "v1", "upsert")])
    res = await test_client_http.post(
        "/data/ingest",
        content=_serialize_batch(batch),
        headers={"Content-Type": "application/vnd.apache.arrow.stream"},
    )
    assert res.status_code == 202
    row = await _poll_for_value(test_client_http, 100, "v1")


async def test_post_ingest_newest_wins(test_client_http: AsyncClient):
    batch_v2 = make_batch([(100, "http", "v2", "upsert")])
    res = await test_client_http.post(
        "/data/ingest",
        content=_serialize_batch(batch_v2),
        headers={"Content-Type": "application/vnd.apache.arrow.stream"},
    )
    assert res.status_code == 202
    row = await _poll_for_value(test_client_http, 100, "v2")


async def test_post_ingest_tombstone(test_client_http: AsyncClient):
    delete_batch = make_batch([(100, "http", "v2", "delete")])
    res = await test_client_http.post(
        "/data/ingest",
        content=_serialize_batch(delete_batch),
        headers={"Content-Type": "application/vnd.apache.arrow.stream"},
    )
    assert res.status_code == 202
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        body = (await test_client_http.get("/data/cache?limit=100")).json()
        if 100 not in {r["id"] for r in body["rows"]}:
            return
        await asyncio.sleep(0.05)
    raise AssertionError("id=100 still present after delete")


async def test_post_ingest_invalid_body_returns_400(test_client_http: AsyncClient):
    res = await test_client_http.post(
        "/data/ingest",
        content=b"not arrow ipc",
        headers={"Content-Type": "application/vnd.apache.arrow.stream"},
    )
    assert res.status_code == 400


async def test_lsm_query_limit_zero_returns_all_rows():
    """limit=0 must not silently return an empty result."""
    from persistence.stream_store.lsm_store import LSMStore
    from tests.publishers.flight_server import make_batch

    store = LSMStore(flush_rows=100, compaction_runs=4)
    store.ingest(make_batch([(1, "a", "v1", "upsert"), (2, "b", "v2", "upsert")]))
    rows, total = store.query(limit=0)
    assert total == 2
    assert len(rows) == 2


async def test_post_ingest_oversized_body_returns_413(test_client_http: AsyncClient):
    payload = b"\x00" * (17 * 1024 * 1024)   # > default 16 MiB
    res = await test_client_http.post(
        "/data/ingest",
        content=payload,
        headers={"Content-Type": "application/vnd.apache.arrow.stream"},
    )
    assert res.status_code == 413


async def test_reinsert_after_delete_wins_over_http(test_client_http: AsyncClient):
    # upsert -> delete -> re-upsert through the real ingest endpoint; the latest
    # write must win even after the delete has been compacted away.
    for value, op in [("v1", "upsert"), ("v1", "delete"), ("v2", "upsert")]:
        batch = make_batch([(500, "rk", value, op)])
        res = await test_client_http.post(
            "/data/ingest",
            content=_serialize_batch(batch),
            headers={"Content-Type": "application/vnd.apache.arrow.stream"},
        )
        assert res.status_code == 202
    row = await _poll_for_value(test_client_http, 500, "v2")
    assert row["value"] == "v2"


async def test_http_ingest_propagates_request_id_to_store_write(
    test_client_http: AsyncClient, cid_caplog,
):
    import logging
    batch = make_batch([(201, "http", "v1", "upsert")])
    with cid_caplog.at_level(logging.DEBUG):
        res = await test_client_http.post(
            "/data/ingest",
            content=_serialize_batch(batch),
            headers={
                "Content-Type": "application/vnd.apache.arrow.stream",
                "X-Request-ID": "req-xyz",
            },
        )
    assert res.status_code == 202
    ingest_logs = [r for r in cid_caplog.records if "ingested batch" in r.getMessage()]
    assert ingest_logs and any(r.correlation_id == "req-xyz" for r in ingest_logs)


async def test_oversized_decoded_batch_is_dropped(
    postgres_container, clickhouse_container, test_clickhouse_client,
    redis_container, empty_flight_server, caplog,
):
    pg_port = int(postgres_container.get_exposed_port(5432))
    ch_port = int(clickhouse_container.get_exposed_port(8123))
    redis_port = int(redis_container.get_exposed_port(6379))
    settings = Settings(
        status="testing",
        postgres_url=f"postgresql://{postgres_container.username}:{postgres_container.password}@localhost:{pg_port}/{postgres_container.dbname}",
        clickhouse_host="localhost", clickhouse_port=ch_port,
        clickhouse_user=clickhouse_container.username or "default",
        clickhouse_password=clickhouse_container.password or "",
        clickhouse_database="default",
        redis_url=f"redis://localhost:{redis_port}/0",
        ingest_transport="flight", flight_host="localhost",
        flight_port=empty_flight_server.port, flight_ticket="items",
        lsm_flush_rows=2, lsm_compaction_runs=2,
        max_request_body_bytes=16 * 1024 * 1024,   # large: let the body reach the handler
        max_ingest_batch_bytes=1,                  # tiny: any decoded batch is "oversized"
        ingest_max_disconnect_seconds=None,
    )
    batch = make_batch([(300, "big", "v1", "upsert")])
    async with lifespan_test_client(settings) as client:
        with caplog.at_level(logging.ERROR):
            res = await client.post(
                "/data/ingest",
                content=_serialize_batch(batch),
                headers={"Content-Type": "application/vnd.apache.arrow.stream"},
            )
        assert res.status_code == 202                       # endpoint accepts the request
        body = (await client.get("/data/cache?limit=100")).json()
        assert body["total"] == 0                           # but the batch was dropped
    assert any("exceeds max_ingest_batch_bytes" in r.message for r in caplog.records)
