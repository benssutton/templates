import pytest
from httpx import ASGITransport, AsyncClient

from main import create_app
from settings import Settings


@pytest.mark.asyncio
async def test_middleware_echoes_inbound_id():
    app = create_app(Settings())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.get("/", headers={"X-Request-ID": "trace-42"})
    assert r.headers["X-Request-ID"] == "trace-42"


@pytest.mark.asyncio
async def test_middleware_generates_id_when_absent():
    app = create_app(Settings())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.get("/")
    assert len(r.headers["X-Request-ID"]) == 32    # uuid4().hex is always 32 hex chars


@pytest.mark.asyncio
async def test_timed_records_boundary_sample_when_active():
    from core.boundary_timing import boundary_samples_var
    from core.correlation import timed

    token = boundary_samples_var.set([])
    try:
        async with timed("clickhouse.select"):
            pass
        samples = boundary_samples_var.get()
        assert len(samples) == 1
        assert samples[0][0] == "clickhouse.select"
        assert samples[0][1] >= 0.0
    finally:
        boundary_samples_var.reset(token)


@pytest.mark.asyncio
async def test_timed_is_silent_when_no_request_active():
    from core.boundary_timing import boundary_samples_var
    from core.correlation import timed

    assert boundary_samples_var.get() is None
    async with timed("clickhouse.select"):   # must not raise
        pass
    assert boundary_samples_var.get() is None
