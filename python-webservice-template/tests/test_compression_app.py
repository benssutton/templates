"""Compression as wired by create_app: position in the stack, and strict-mode
exemptions on the real probe, metrics and MCP routes."""

import json

from httpx import ASGITransport, AsyncClient
from pydantic import SecretStr

from core.zstd_compat import zstd
from main import create_app
from settings import Settings
from tests.app_client import lifespan_test_client
from tests.publishers.batches import make_batch, serialize_batch
from tests.support.settings import app_settings


async def test_openapi_document_is_served_as_zstd() -> None:
    app = create_app(Settings())
    async with (
        AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c,
        c.stream("GET", "/openapi.json", headers={"accept-encoding": "zstd"}) as r,
    ):
        raw = b"".join([chunk async for chunk in r.aiter_raw()])
    assert r.headers["content-encoding"] == "zstd"
    assert "openapi" in json.loads(zstd.decompress(raw))


async def test_strict_mode_refuses_before_routing_and_skips_preflight() -> None:
    app = create_app(Settings(compression_require_zstd=True))
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        refused = await c.get("/openapi.json", headers={"accept-encoding": "gzip"})
        preflight = await c.options(
            "/data",
            headers={
                "Origin": "https://app.example.com",
                "Access-Control-Request-Method": "GET",
                "accept-encoding": "identity",
            },
        )
    assert refused.status_code == 406
    assert refused.json() == {"detail": "Accept-Encoding must allow zstd"}
    assert preflight.status_code == 200


async def test_strict_mode_answers_406_before_authentication() -> None:
    app = create_app(
        Settings(
            compression_require_zstd=True,
            auth_enabled=True,
            auth_jwt_secret=SecretStr("s" * 32),
        )
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.get("/data", headers={"accept-encoding": "gzip"})
    assert r.status_code == 406


async def test_strict_mode_exempts_the_real_probe_metrics_and_mcp_routes(
    postgres_container, clickhouse_container, test_clickhouse_client, redis_container
) -> None:
    settings = app_settings(
        postgres_container,
        clickhouse_container,
        redis_container,
        compression_require_zstd=True,
    )
    async with lifespan_test_client(settings) as client:
        plain = {"accept-encoding": "identity"}
        assert (await client.get("/health/live", headers=plain)).status_code == 200
        assert (await client.get("/health/ready", headers=plain)).status_code == 200
        assert (await client.get("/metrics", headers=plain)).status_code == 200
        ping = await client.post(
            "/mcp/",
            headers={**plain, "Accept": "application/json, text/event-stream"},
            json={"jsonrpc": "2.0", "id": 1, "method": "ping"},
        )
        assert ping.status_code != 406
        assert (await client.get("/data", headers=plain)).status_code == 406


async def test_zstd_arrow_body_is_accepted_by_ingest(test_client) -> None:
    body = serialize_batch(make_batch([(7001, "zstd", "v1", "upsert")]))
    r = await test_client.post(
        "/data/ingest",
        content=zstd.compress(body),
        headers={
            "Content-Type": "application/vnd.apache.arrow.stream",
            "Content-Encoding": "zstd",
        },
    )
    assert r.status_code == 202
    assert r.json() == {"accepted": True}
