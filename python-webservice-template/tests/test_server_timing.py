import pytest

pytestmark = pytest.mark.observability


async def test_data_read_emits_server_timing(test_client):
    r = await test_client.get("/data?limit=5")
    assert r.status_code == 200
    server_timing = r.headers.get("Server-Timing", "")
    assert "clickhouse_select" in server_timing
    assert "total" in server_timing


async def test_request_without_boundaries_has_only_total(test_client):
    # /health/live has no instrumented boundaries, so its Server-Timing must
    # carry only `total`. (The request-scoped isolation invariant itself is
    # proven rigorously in tests/test_boundary_timing.py and test_correlation.py.)
    r = await test_client.get("/health/live")
    assert r.status_code == 200
    server_timing = r.headers.get("Server-Timing", "")
    assert "total" in server_timing
    for token in ("clickhouse", "lsm", "ingest", "postgres"):
        assert token not in server_timing


async def test_cache_read_emits_lsm_boundary(test_client):
    r = await test_client.get("/data/cache?limit=5")
    assert r.status_code == 200
    assert "lsm_query" in r.headers.get("Server-Timing", "")


async def test_http_ingest_emits_decode_and_write_boundaries(test_client):
    import pyarrow as pa
    import pyarrow.ipc as pa_ipc
    from tests.publishers.flight_server import make_batch

    batch = make_batch([(900, "perf", "v1", "upsert")])
    buf = pa.BufferOutputStream()
    with pa_ipc.new_stream(buf, batch.schema) as writer:
        writer.write_batch(batch)
    r = await test_client.post(
        "/data/ingest",
        content=buf.getvalue().to_pybytes(),
        headers={"Content-Type": "application/vnd.apache.arrow.stream"},
    )
    assert r.status_code == 202
    server_timing = r.headers.get("Server-Timing", "")
    assert "ingest_decode" in server_timing
    assert "ingest_lsm_write" in server_timing
