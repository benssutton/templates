import asyncio
import logging
import time

import pyarrow as pa
import pytest
from httpx import AsyncClient

from tests.app_client import lifespan_test_client
from tests.publishers.batches import make_batch, serialize_batch
from tests.support.settings import app_settings

pytestmark = pytest.mark.http_ingest


@pytest.fixture(scope="module")
async def test_client_http(
    postgres_container, clickhouse_container, test_clickhouse_client, redis_container
):
    settings = app_settings(postgres_container, clickhouse_container, redis_container)
    # Isolated app: own container, own MCPServer, own lifespan.
    async with lifespan_test_client(settings) as client:
        yield client


async def _poll_for_value(
    client: AsyncClient, id_: int, expected_value: str, timeout: float = 10.0
) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        body = (await client.get("/data/cache?limit=100")).json()
        rows_by_id = {r["id"]: r for r in body["rows"]}
        if id_ in rows_by_id and rows_by_id[id_]["value"] == expected_value:
            return rows_by_id[id_]
        await asyncio.sleep(0.05)
    raise AssertionError(
        f"id={id_} with value={expected_value!r} never appeared in cache"
    )


async def test_lsm_cache_empty_before_ingest(test_client_http: AsyncClient):
    body = (await test_client_http.get("/data/cache?limit=100")).json()
    assert body["total"] == 0
    assert body["rows"] == []


async def test_post_ingest_upsert_appears_in_cache(test_client_http: AsyncClient):
    batch = make_batch([(100, "http", "v1", "upsert")])
    res = await test_client_http.post(
        "/data/ingest",
        content=serialize_batch(batch),
        headers={"Content-Type": "application/vnd.apache.arrow.stream"},
    )
    assert res.status_code == 202
    await _poll_for_value(test_client_http, 100, "v1")


@pytest.mark.parametrize(
    "content_type",
    ["application/vnd.apache.arrow.stream", "application/octet-stream"],
)
async def test_post_ingest_accepts_both_arrow_media_types(
    test_client_http: AsyncClient, content_type: str
):
    res = await test_client_http.post(
        "/data/ingest",
        content=serialize_batch(make_batch([(101, "http", "v1", "upsert")])),
        headers={"Content-Type": content_type},
    )
    assert res.status_code == 202
    assert res.json() == {"accepted": True}


@pytest.mark.parametrize("content_type", ["text/plain", "application/json", None])
async def test_post_ingest_other_content_type_is_415(
    test_client_http: AsyncClient, content_type: str | None
):
    res = await test_client_http.post(
        "/data/ingest",
        content=serialize_batch(make_batch([(102, "http", "v1", "upsert")])),
        headers={"Content-Type": content_type} if content_type else {},
    )
    assert res.status_code == 415
    assert isinstance(res.json()["detail"], str)


async def test_post_ingest_newest_wins(test_client_http: AsyncClient):
    batch_v2 = make_batch([(100, "http", "v2", "upsert")])
    res = await test_client_http.post(
        "/data/ingest",
        content=serialize_batch(batch_v2),
        headers={"Content-Type": "application/vnd.apache.arrow.stream"},
    )
    assert res.status_code == 202
    await _poll_for_value(test_client_http, 100, "v2")


async def test_post_ingest_tombstone(test_client_http: AsyncClient):
    delete_batch = make_batch([(100, "http", "v2", "delete")])
    res = await test_client_http.post(
        "/data/ingest",
        content=serialize_batch(delete_batch),
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
    # The parser's own message is logged, never echoed to the client.
    assert res.json() == {"detail": "Invalid Arrow IPC stream"}


async def test_lsm_query_limit_zero_returns_all_rows():
    """limit=0 must not silently return an empty result."""
    from persistence.stream_store.lsm_store import LSMStore

    store = LSMStore(flush_rows=100, compaction_runs=4)
    store.ingest(make_batch([(1, "a", "v1", "upsert"), (2, "b", "v2", "upsert")]))
    rows, total = store.query(limit=0)
    assert total == 2
    assert len(rows) == 2


async def test_post_ingest_oversized_body_returns_413(test_client_http: AsyncClient):
    payload = b"\x00" * (17 * 1024 * 1024)  # > default 16 MiB
    res = await test_client_http.post(
        "/data/ingest",
        content=payload,
        headers={"Content-Type": "application/vnd.apache.arrow.stream"},
    )
    assert res.status_code == 413


async def _chunked(total_bytes: int, chunk: int = 1024 * 1024):
    for _ in range(total_bytes // chunk):
        yield b"\x00" * chunk


async def test_post_ingest_chunked_oversized_body_returns_413_and_writes_nothing(
    test_client_http: AsyncClient,
):
    # No Content-Length: the limit must stop the read, not reject after ingesting.
    before = (await test_client_http.get("/data/cache", params={"limit": 1})).json()
    res = await test_client_http.post(
        "/data/ingest",
        content=_chunked(17 * 1024 * 1024),
        headers={"Content-Type": "application/vnd.apache.arrow.stream"},
    )
    assert res.status_code == 413
    after = (await test_client_http.get("/data/cache", params={"limit": 1})).json()
    assert after["total"] == before["total"]


async def test_post_json_route_chunked_oversized_body_returns_413(
    test_client_http: AsyncClient,
):
    # FastAPI body parsing turns non-HTTP errors into 400; the limit must still be 413.
    res = await test_client_http.post(
        "/cache/",
        content=_chunked(17 * 1024 * 1024),
        headers={"Content-Type": "application/json"},
    )
    assert res.status_code == 413


async def test_reinsert_after_delete_wins_over_http(test_client_http: AsyncClient):
    # upsert -> delete -> re-upsert through the real ingest endpoint; the latest
    # write must win even after the delete has been compacted away.
    for value, op in [("v1", "upsert"), ("v1", "delete"), ("v2", "upsert")]:
        batch = make_batch([(500, "rk", value, op)])
        res = await test_client_http.post(
            "/data/ingest",
            content=serialize_batch(batch),
            headers={"Content-Type": "application/vnd.apache.arrow.stream"},
        )
        assert res.status_code == 202
    row = await _poll_for_value(test_client_http, 500, "v2")
    assert row["value"] == "v2"


async def test_http_ingest_propagates_request_id_to_store_write(
    test_client_http: AsyncClient,
    cid_caplog,
):
    batch = make_batch([(201, "http", "v1", "upsert")])
    with cid_caplog.at_level(logging.DEBUG):
        res = await test_client_http.post(
            "/data/ingest",
            content=serialize_batch(batch),
            headers={
                "Content-Type": "application/vnd.apache.arrow.stream",
                "X-Request-ID": "req-xyz",
            },
        )
    assert res.status_code == 202
    ingest_logs = [r for r in cid_caplog.records if "ingested batch" in r.getMessage()]
    assert ingest_logs and any(r.correlation_id == "req-xyz" for r in ingest_logs)


async def test_oversized_decoded_batch_returns_413_and_writes_nothing(
    postgres_container,
    clickhouse_container,
    test_clickhouse_client,
    redis_container,
    caplog,
):
    settings = app_settings(
        postgres_container,
        clickhouse_container,
        redis_container,
        max_request_body_bytes=16
        * 1024
        * 1024,  # large: let the body reach the handler
        max_ingest_batch_bytes=1,  # tiny: any decoded batch is "oversized"
    )
    batch = make_batch([(300, "big", "v1", "upsert")])
    async with lifespan_test_client(settings) as client:
        with caplog.at_level(logging.ERROR):
            res = await client.post(
                "/data/ingest",
                content=serialize_batch(batch),
                headers={"Content-Type": "application/vnd.apache.arrow.stream"},
            )
        assert res.status_code == 413
        assert res.json() == {"detail": "Payload Too Large"}
        body = (await client.get("/data/cache?limit=100")).json()
        assert body["total"] == 0
    assert any("exceeds max_ingest_batch_bytes" in r.message for r in caplog.records)


def _stream(*batches: pa.RecordBatch) -> bytes:
    buf = pa.BufferOutputStream()
    with pa.ipc.new_stream(buf, batches[0].schema) as writer:
        for b in batches:
            writer.write_batch(b)
    return buf.getvalue().to_pybytes()


async def test_post_ingest_oversized_second_batch_writes_nothing(
    postgres_container, clickhouse_container, test_clickhouse_client, redis_container
):
    # One IPC stream has one schema, so per-batch rejection is about size: the
    # oversized second batch must keep the first one out of the store too.
    small = make_batch([(800, "s", "v", "upsert")])
    big = make_batch([(801 + i, "b" * 50, "v" * 50, "upsert") for i in range(50)])
    settings = app_settings(
        postgres_container,
        clickhouse_container,
        redis_container,
        max_ingest_batch_bytes=small.nbytes,
    )
    async with lifespan_test_client(settings) as client:
        res = await client.post(
            "/data/ingest",
            content=_stream(small, big),
            headers={"Content-Type": "application/vnd.apache.arrow.stream"},
        )
        assert res.status_code == 413
        assert (await client.get("/data/cache?limit=100")).json()["total"] == 0


async def test_post_ingest_unmergeable_batch_returns_422(test_client_http: AsyncClient):
    bad = pa.record_batch({"name": ["x"], "value": ["v"], "op": ["upsert"]})
    res = await test_client_http.post(
        "/data/ingest",
        content=serialize_batch(bad),
        headers={"Content-Type": "application/vnd.apache.arrow.stream"},
    )
    assert res.status_code == 422
    assert "missing required columns" in res.json()["detail"]


async def test_ingest_counters_are_prometheus_counters(
    postgres_container, clickhouse_container, test_clickhouse_client, redis_container
):
    settings = app_settings(postgres_container, clickhouse_container, redis_container)
    unmergeable = pa.record_batch({"name": ["x"], "value": ["v"], "op": ["upsert"]})
    async with lifespan_test_client(settings) as client:
        for payload in (
            serialize_batch(
                make_batch([(900, "a", "v", "upsert"), (901, "b", "v", "upsert")])
            ),
            serialize_batch(unmergeable),
        ):
            await client.post(
                "/data/ingest",
                content=payload,
                headers={"Content-Type": "application/vnd.apache.arrow.stream"},
            )
        text = (await client.get("/metrics")).text
    assert "# TYPE ingest_rows_ingested_total counter" in text
    assert "ingest_rows_ingested_total 2.0" in text
    assert 'ingest_outcomes_total{outcome="accepted"} 1.0' in text
    assert 'ingest_outcomes_total{outcome="rejected"} 1.0' in text
    assert 'ingest_outcomes_total{outcome="failed"} 0.0' in text
    # process_* belongs to prometheus_client's standard ProcessCollector.
    assert not any(
        line.startswith(("process_cpu_percent", "process_memory_rss_bytes"))
        for line in text.splitlines()
    )
