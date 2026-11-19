"""Every error status answers application/json {"detail": ...}."""

import pytest
from httpx import ASGITransport, AsyncClient

from main import create_app
from settings import Settings


def _assert_detail(response, status: int) -> None:
    assert response.status_code == status
    assert response.headers["content-type"].startswith("application/json")
    assert isinstance(response.json()["detail"], (str, list))


async def _client(app) -> AsyncClient:
    # raise_app_exceptions=False: Starlette re-raises an unhandled error after
    # sending the 500, as a real server would log it.
    return AsyncClient(
        transport=ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://localhost",
    )


async def test_unknown_path_is_json_404():
    async with await _client(create_app(Settings())) as c:
        _assert_detail(await c.get("/no/such/path"), 404)


async def test_wrong_method_is_json_405():
    async with await _client(create_app(Settings())) as c:
        _assert_detail(await c.delete("/health/live"), 405)


async def test_malformed_json_body_is_400(test_client):
    response = await test_client.post(
        "/cache",
        content=b'{"key": "k", "value": ',
        headers={"Content-Type": "application/json"},
    )
    _assert_detail(response, 400)
    assert response.json() == {"detail": "Malformed JSON body"}


@pytest.mark.parametrize("content_type", ["text/plain", None])
async def test_json_route_with_another_content_type_is_415(test_client, content_type):
    headers = {"Content-Type": content_type} if content_type else {}
    response = await test_client.post(
        "/cache", content=b'{"key": "k", "value": 1}', headers=headers
    )
    _assert_detail(response, 415)


async def test_invalid_json_body_is_still_422(test_client):
    response = await test_client.post("/cache", json={"key": "", "value": 1})
    _assert_detail(response, 422)
    assert isinstance(response.json()["detail"], list)


async def test_unhandled_error_is_json_500_without_internals():
    app = create_app(Settings())

    @app.get("/boom")
    async def boom():
        raise RuntimeError("secret internals")

    async with await _client(app) as c:
        response = await c.get("/boom")
    _assert_detail(response, 500)
    assert response.json() == {"detail": "Internal Server Error"}


@pytest.mark.parametrize(
    ("headers", "status"),
    [
        ({"Host": "evil.example"}, 403),
        ({"Host": "localhost", "Origin": "https://evil.example"}, 403),
    ],
)
async def test_mcp_host_and_origin_rejections_are_json(test_client, headers, status):
    # The SDK's own plain-text rejections (421 for a foreign Host), rewritten by
    # JsonErrorBodyMiddleware to the contract's JSON 403.
    response = await test_client.post(
        "/mcp/",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            **headers,
        },
        json={"jsonrpc": "2.0", "id": 1, "method": "ping"},
    )
    _assert_detail(response, status)
