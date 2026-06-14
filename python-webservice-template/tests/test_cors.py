import pytest
from httpx import ASGITransport, AsyncClient

from main import create_app
from settings import Settings


@pytest.mark.asyncio
async def test_cors_preflight_allows_configured_origin():
    # CORS is wired at app construction, before startup — no lifespan/containers
    # needed to exercise a real preflight request end to end.
    app = create_app(Settings(cors_allow_origins=["https://app.example.com"]))
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.options(
            "/data",
            headers={
                "Origin": "https://app.example.com",
                "Access-Control-Request-Method": "GET",
            },
        )
    assert r.status_code == 200
    assert r.headers["access-control-allow-origin"] == "https://app.example.com"


@pytest.mark.asyncio
async def test_cors_preflight_wildcard_default_allows_any_origin():
    # Default settings use origins=["*"] — any origin should be reflected back.
    app = create_app(Settings())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.options(
            "/data",
            headers={
                "Origin": "https://arbitrary.example.com",
                "Access-Control-Request-Method": "GET",
            },
        )
    assert r.status_code == 200
    assert r.headers["access-control-allow-origin"] == "*"


def test_cors_credentials_with_wildcard_origins_raises():
    with pytest.raises(Exception, match="incompatible"):
        Settings(cors_allow_credentials=True, cors_allow_origins=["*"])
