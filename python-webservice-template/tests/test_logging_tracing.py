import asyncio
import json
import logging

import uvicorn
from httpx import ASGITransport, AsyncClient
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

from core.correlation import correlation_id_var
from core.logging_config import JsonFormatter
from core.tracing import configure_tracing
from main import create_app
from settings import Settings
from tests.support.settings import app_settings


def test_log_lines_are_json_with_the_correlation_id(cid_caplog):
    token = correlation_id_var.set("req-json-1")
    try:
        with cid_caplog.at_level(logging.INFO, logger="tests.json"):
            try:
                raise ValueError("boom")
            except ValueError:
                logging.getLogger("tests.json").exception("failed %s", "here")
    finally:
        correlation_id_var.reset(token)
    [record] = [r for r in cid_caplog.records if r.name == "tests.json"]
    entry = json.loads(JsonFormatter().format(record))
    assert entry["message"] == "failed here"
    assert entry["level"] == "ERROR"
    assert entry["logger"] == "tests.json"
    assert entry["correlation_id"] == "req-json-1"
    assert "ValueError: boom" in entry["exception"]
    assert entry["timestamp"].endswith("+00:00")


async def test_uvicorn_access_log_carries_the_request_id(
    postgres_container,
    clickhouse_container,
    test_clickhouse_client,
    redis_container,
    cid_caplog,
):
    # A real uvicorn server (plain HTTP, ephemeral port) with main.py's log setup.
    app = create_app(
        app_settings(postgres_container, clickhouse_container, redis_container)
    )
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=0, log_config=None)
    )
    serving = asyncio.create_task(server.serve())
    try:
        while not server.started:
            if serving.done():
                serving.result()
            await asyncio.sleep(0.05)
        port = server.servers[0].sockets[0].getsockname()[1]
        with cid_caplog.at_level(logging.INFO, logger="uvicorn.access"):
            async with AsyncClient(base_url=f"http://127.0.0.1:{port}") as client:
                response = await client.get(
                    "/health/live", headers={"X-Request-ID": "access-42"}
                )
        assert response.status_code == 200
    finally:
        server.should_exit = True
        await serving
    access = [r for r in cid_caplog.records if r.name == "uvicorn.access"]
    assert access and all(r.correlation_id == "access-42" for r in access)


async def test_tracing_is_off_by_default():
    assert configure_tracing(create_app(Settings()), Settings()) is None


async def test_traceparent_is_continued():
    exporter = InMemorySpanExporter()
    app = create_app(Settings())
    provider = configure_tracing(app, Settings(), exporter=exporter)
    assert provider is not None
    trace_id = "4bf92f3577b34da6a3ce929d0e0e4736"
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        response = await c.get(
            "/", headers={"traceparent": f"00-{trace_id}-00f067aa0ba902b7-01"}
        )
    assert response.status_code == 200
    spans = exporter.get_finished_spans()
    assert spans
    contexts = [s.get_span_context() for s in spans]
    assert {format(c.trace_id, "032x") for c in contexts if c is not None} == {trace_id}
    provider.shutdown()
