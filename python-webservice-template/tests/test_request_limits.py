import pytest
from httpx import ASGITransport, AsyncClient
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route

from core.request_limits import MaxBodySizeMiddleware


def _app(limit: int) -> Starlette:
    async def echo(request):
        body = await request.body()
        return PlainTextResponse(f"got {len(body)}")

    app = Starlette(routes=[Route("/echo", echo, methods=["POST"])])
    app.add_middleware(MaxBodySizeMiddleware, max_bytes=limit)
    return app


async def test_body_within_limit_passes():
    async with AsyncClient(transport=ASGITransport(app=_app(1024)), base_url="http://t") as c:
        r = await c.post("/echo", content=b"x" * 512)
    assert r.status_code == 200
    assert r.text == "got 512"


async def test_body_over_limit_returns_413():
    async with AsyncClient(transport=ASGITransport(app=_app(1024)), base_url="http://t") as c:
        r = await c.post("/echo", content=b"x" * 2048)
    assert r.status_code == 413
