import pytest
from httpx import ASGITransport, AsyncClient

from core.correlation import correlation_id_var, get_correlation_id, set_correlation_id
from main import create_app
from settings import Settings


@pytest.mark.asyncio
async def test_get_correlation_id_returns_current_value():
    token = set_correlation_id("abc123")
    try:
        assert get_correlation_id() == "abc123"
    finally:
        correlation_id_var.reset(token)


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
    assert len(r.headers["X-Request-ID"]) == 32  # uuid4().hex is always 32 hex chars


@pytest.mark.parametrize(
    "inbound",
    ["", "x" * 129, "has space", "new\nline", "semi;colon", "ünïcode"],
)
async def test_middleware_replaces_unsafe_inbound_id(inbound):
    app = create_app(Settings())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.get("/", headers={"X-Request-ID": inbound.encode()})
    assert r.headers["X-Request-ID"] != inbound
    assert len(r.headers["X-Request-ID"]) == 32


async def test_middleware_adopts_max_length_safe_id():
    inbound = "A-z_0.9" + "x" * 121  # 128 chars of the allowed alphabet
    app = create_app(Settings())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.get("/", headers={"X-Request-ID": inbound})
    assert r.headers["X-Request-ID"] == inbound
