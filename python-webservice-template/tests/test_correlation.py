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
    assert len(r.headers["X-Request-ID"]) >= 8     # generated UUID hex
