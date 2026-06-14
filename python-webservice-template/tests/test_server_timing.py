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
