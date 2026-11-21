"""Every route has one path; a trailing slash reaches the same handler, no redirect."""

from urllib.parse import quote

import pytest
from starlette.types import Message, Receive, Scope, Send

from core.canonical_path import CanonicalPathMiddleware
from tests.support.settings import unique


@pytest.mark.parametrize("path", ["/config", "/config/"])
async def test_collection_route_with_and_without_slash(test_client, path):
    response = await test_client.get(path, follow_redirects=False)
    assert response.status_code == 200


async def test_mcp_without_slash_is_served_not_redirected(test_client):
    response = await test_client.post(
        "/mcp",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        },
        json={"jsonrpc": "2.0", "id": 1, "method": "ping"},
        follow_redirects=False,
    )
    assert response.status_code != 307


async def test_openapi_lists_each_route_once(test_client):
    paths = (await test_client.get("/openapi.json")).json()["paths"]
    assert not [p for p in paths if p != "/" and p.endswith("/")]


async def test_encoded_trailing_slash_is_part_of_the_key(test_client):
    """`a%2F` is the key `a/`, not `a` with a trailing slash."""
    key = unique("slash") + "/"
    posted = await test_client.post("/cache", json={"key": key, "value": 1})
    assert posted.status_code == 201

    response = await test_client.get(
        "/cache/" + quote(key, safe=""), follow_redirects=False
    )
    assert response.status_code == 200
    assert response.json()["key"] == key


async def _routed_scope(raw_path: bytes, path: str) -> Scope:
    seen: list[Scope] = []

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        seen.append(scope)

    async def receive() -> Message:
        return {"type": "http.request"}

    async def send(message: Message) -> None:
        pass

    scope: Scope = {"type": "http", "path": path, "raw_path": raw_path}
    await CanonicalPathMiddleware(app)(scope, receive, send)
    return seen[0]


@pytest.mark.parametrize(
    ("raw_path", "path", "routed_raw", "routed"),
    [
        (b"/cache/a%2F/", "/cache/a//", b"/cache/a%2F", "/cache/a/"),
        (b"/cache/a%2F", "/cache/a/", b"/cache/a%2F", "/cache/a/"),
        (b"/cache/caf%C3%A9//", "/cache/café//", b"/cache/caf%C3%A9", "/cache/café"),
        (b"/mcp", "/mcp", b"/mcp/", "/mcp/"),
        (b"/", "/", b"/", "/"),
    ],
)
async def test_raw_path_keeps_its_percent_encoding(raw_path, path, routed_raw, routed):
    scope = await _routed_scope(raw_path, path)
    assert (scope["raw_path"], scope["path"]) == (routed_raw, routed)
