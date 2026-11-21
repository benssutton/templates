"""Every route has one path; a trailing slash reaches the same handler, no redirect."""

import pytest


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
