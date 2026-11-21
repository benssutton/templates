import json
from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from main import mcp_transport_security
from settings import Settings
from tests.app_client import lifespan_test_client
from tests.support.settings import app_settings


def parse_mcp_response(response: httpx.Response) -> dict[str, Any]:
    ct = response.headers.get("content-type", "")
    if "text/event-stream" in ct:
        r = None
        for line in response.text.splitlines():
            if line.startswith("data:"):
                r = json.loads(line[5:].strip())
    else:
        r = response.json()
    assert r is not None, f"no JSON-RPC payload in SSE response: {response.text!r}"
    return r


_INIT = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
        "protocolVersion": "2025-03-26",
        "capabilities": {},
        "clientInfo": {"name": "test-client", "version": "0.1"},
    },
}


@pytest.mark.parametrize(
    ("extra_headers", "expected"),
    [
        ({"Host": "localhost"}, 200),  # https on 443 sends no port
        ({"Host": "localhost:8443"}, 200),
        ({"Host": "localhost", "Origin": "https://localhost"}, 200),
        ({"Host": "evil.example"}, 403),
        ({"Host": "localhost", "Origin": "https://evil.example"}, 403),
    ],
)
async def test_mcp_host_and_origin_allow_list(test_client, extra_headers, expected):
    response = await test_client.post(
        "/mcp/",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            **extra_headers,
        },
        json=_INIT,
    )
    assert response.status_code == expected


def test_mcp_transport_security_expands_configured_hosts():
    security = mcp_transport_security(
        Settings(mcp_allowed_hosts=["app", "api.example.com:8443"])
    )
    assert security.allowed_hosts == ["app", "app:*", "api.example.com:8443"]
    assert "https://app:*" in security.allowed_origins
    assert "https://api.example.com:8443" in security.allowed_origins


def test_mcp_allowed_hosts_must_not_be_empty():
    with pytest.raises(ValidationError):
        Settings(mcp_allowed_hosts=[])


async def test_mcp(test_client):

    url = "/mcp/"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }

    # ── Initialize session ────────────────────────────────────────────────────
    msg_id = 1
    response = await test_client.post(
        url,
        headers=headers,
        json={
            "jsonrpc": "2.0",
            "id": msg_id,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": {"name": "test-client", "version": "0.1"},
            },
        },
    )
    assert response.status_code == 200
    r = parse_mcp_response(response)
    assert r is not None

    mcp_session_id = response.headers.get("mcp-session-id")
    session_headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "mcp-session-id": mcp_session_id,
    }

    # ── List tools ────────────────────────────────────────────────────────────
    msg_id += 1
    response = await test_client.post(
        url,
        headers=session_headers,
        json={
            "jsonrpc": "2.0",
            "method": "tools/list",
            "id": msg_id,
        },
    )
    assert response.status_code == 200
    r = parse_mcp_response(response)
    tool_names = {tool["name"] for tool in r["result"]["tools"]}
    assert "get_health_status" in tool_names

    # ── Call get_health_status and assert on result content ───────────────────
    # The test app is started with status="testing" (see conftest.py Settings).
    msg_id += 1
    response = await test_client.post(
        url,
        headers=session_headers,
        json={
            "jsonrpc": "2.0",
            "method": "tools/call",
            "id": msg_id,
            "params": {"name": "get_health_status", "arguments": {}},
        },
    )
    assert response.status_code == 200
    r = parse_mcp_response(response)
    assert "result" in r, f"Expected result, got: {r}"
    assert r["result"]["isError"] is False
    content = r["result"]["content"]
    assert isinstance(content, list) and len(content) > 0
    assert "testing" in content[0]["text"]

    # ── Call all other tools with the correct JSON-RPC method ─────────────────
    for tool_name in tool_names - {"get_health_status"}:
        msg_id += 1
        response = await test_client.post(
            url,
            headers=session_headers,
            json={
                "jsonrpc": "2.0",
                "method": "tools/call",
                "id": msg_id,
                "params": {"name": tool_name, "arguments": {}},
            },
        )
        assert response.status_code == 200
        r = parse_mcp_response(response)
        assert "result" in r, f"Tool {tool_name!r} returned an error: {r}"


async def test_mcp_chunked_oversized_body_returns_413(
    postgres_container, clickhouse_container, test_clickhouse_client, redis_container
):
    # The SDK catches the exception raised from receive() and answers 500 itself;
    # the body limit must still win. A 1 KiB limit trips before the SDK's own
    # (larger) message-size check.
    settings = app_settings(
        postgres_container,
        clickhouse_container,
        redis_container,
        max_request_body_bytes=1024,
    )

    async def chunks():
        for _ in range(10):
            yield b" " * 512

    async with lifespan_test_client(settings) as client:
        response = await client.post(
            "/mcp/",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json, text/event-stream",
            },
            content=chunks(),
        )
    assert response.status_code == 413
    assert response.json() == {"detail": "Payload Too Large"}


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ('["localhost","app"]', ["localhost", "app"]),
        (
            "localhost, app ,api.example.com:8443",
            ["localhost", "app", "api.example.com:8443"],
        ),
        ("localhost", ["localhost"]),
    ],
)
def test_mcp_allowed_hosts_accepts_json_or_comma_separated(tmp_path, raw, expected):
    # Read through a real env file, the same path a deployment's env var takes.
    env_file = tmp_path / ".env"
    env_file.write_text(f"MCP_ALLOWED_HOSTS={raw}\n")
    settings = Settings(_env_file=env_file)  # pyright: ignore[reportCallIssue]
    assert settings.mcp_allowed_hosts == expected


async def _call_tool_raw(client, headers, name: str, arguments: dict) -> dict:
    response = await client.post(
        "/mcp/",
        headers=headers,
        json={
            "jsonrpc": "2.0",
            "id": 7,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        },
    )
    assert response.status_code == 200
    return parse_mcp_response(response)["result"]


async def _call_tool(client, headers, name: str, arguments: dict) -> Any:
    """Call a tool; the result is one text item holding the REST body as JSON."""
    result = await _call_tool_raw(client, headers, name, arguments)
    assert result["isError"] is False, result
    assert len(result["content"]) == 1, result
    assert result["content"][0]["type"] == "text"
    return json.loads(result["content"][0]["text"])


async def test_mcp_tools_mirror_rest_endpoints(test_client):
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }
    init = await test_client.post("/mcp/", headers=headers, json=_INIT)
    headers["mcp-session-id"] = init.headers["mcp-session-id"]
    await test_client.post("/config", json={"key": "mcp_key", "value": "v"})
    await test_client.post("/cache", json={"key": "mcp_key", "value": {"a": 1}})

    status = await _call_tool(test_client, headers, "get_health_status", {})
    assert status["app"]["status"] == "testing"
    assert {d["name"] for d in status["dependencies"]} == {
        "postgres",
        "clickhouse",
        "redis",
    }

    configs = await _call_tool(test_client, headers, "get_config", {})
    assert configs == (await test_client.get("/config")).json()
    assert {"key": "mcp_key", "value": "v"} in configs

    entry = await _call_tool(test_client, headers, "get_cache", {"key": "mcp_key"})
    assert entry["value"] == {"a": 1}

    # Absent key: REST answers 404, the tool reports an error result with the same body.
    missing = await _call_tool_raw(test_client, headers, "get_cache", {"key": "absent"})
    assert missing["isError"] is True
    assert len(missing["content"]) == 1
    assert json.loads(missing["content"][0]["text"]) == {
        "detail": "Key 'absent' not found"
    }

    rest = (await test_client.get("/data", params={"limit": 2})).json()
    assert await _call_tool(test_client, headers, "get_data", {"limit": 2}) == rest

    cached = await _call_tool(test_client, headers, "get_cached_data", {"limit": 5})
    assert cached == (await test_client.get("/data/cache", params={"limit": 5})).json()


async def test_mcp_tool_names(test_client):
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }
    init = await test_client.post("/mcp/", headers=headers, json=_INIT)
    headers["mcp-session-id"] = init.headers["mcp-session-id"]
    response = await test_client.post(
        "/mcp/",
        headers=headers,
        json={"jsonrpc": "2.0", "id": 3, "method": "tools/list"},
    )
    names = sorted(t["name"] for t in parse_mcp_response(response)["result"]["tools"])
    assert names == [
        "get_cache",
        "get_cached_data",
        "get_config",
        "get_data",
        "get_health_status",
    ]
