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
    async with AsyncClient(
        transport=ASGITransport(app=_app(1024)), base_url="http://t"
    ) as c:
        r = await c.post("/echo", content=b"x" * 512)
    assert r.status_code == 200
    assert r.text == "got 512"


async def test_body_over_limit_returns_413():
    # Fixed bytes => httpx sets Content-Length, so this exercises the
    # Content-Length fast-path rejection.
    async with AsyncClient(
        transport=ASGITransport(app=_app(1024)), base_url="http://t"
    ) as c:
        r = await c.post("/echo", content=b"x" * 2048)
    assert r.status_code == 413


async def test_chunked_body_over_limit_returns_413_without_running_handler():
    # A generator body has no known length, so httpx sends Transfer-Encoding:
    # chunked with NO Content-Length. This skips the fast path and exercises the
    # streaming check: the read is aborted as soon as the limit is crossed, so the
    # handler never sees the body and the rest of the stream is never pulled.
    handled: list[int] = []
    chunks_sent = 0

    async def record(request):
        body = await request.body()
        handled.append(len(body))  # the side effect a real ingest would perform
        return PlainTextResponse("ok")

    app = Starlette(routes=[Route("/echo", record, methods=["POST"])])
    app.add_middleware(MaxBodySizeMiddleware, max_bytes=1024)

    async def gen():
        nonlocal chunks_sent
        for _ in range(100):
            chunks_sent += 1
            yield b"x" * 512  # 50 KiB total, streamed, no Content-Length

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.post("/echo", content=gen())
    assert r.status_code == 413
    assert handled == []
    assert chunks_sent < 100


async def test_fast_path_413_is_json_detail():
    async with AsyncClient(
        transport=ASGITransport(app=_app(1024)), base_url="http://t"
    ) as c:
        r = await c.post("/echo", content=b"x" * 2048)
    assert r.status_code == 413
    assert r.headers["content-type"] == "application/json"
    assert r.json() == {"detail": "Payload Too Large"}


async def test_chunked_413_survives_a_handler_that_swallows_the_error():
    # The MCP SDK catches every exception from receive() and answers 500 itself;
    # the limit must still produce the 413.
    async def swallow(request):
        try:
            await request.body()
        except Exception:
            return PlainTextResponse("swallowed", status_code=500)
        return PlainTextResponse("ok")

    app = Starlette(routes=[Route("/echo", swallow, methods=["POST"])])
    app.add_middleware(MaxBodySizeMiddleware, max_bytes=1024)

    async def gen():
        for _ in range(10):
            yield b"x" * 512

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.post("/echo", content=gen())
    assert r.status_code == 413
    assert r.json() == {"detail": "Payload Too Large"}
