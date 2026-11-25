"""The content-coding middlewares on a minimal app: real HTTP through ASGITransport, raw
(undecoded) bodies, so each test sees exactly what a client receives."""

import gzip
import json
import os
from typing import Any

from httpx import ASGITransport, AsyncClient
from httpx import Response as HttpxResponse
from starlette.applications import Starlette
from starlette.datastructures import Headers
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route
from starlette.types import Message, Receive, Scope, Send

from core.compression import ResponseCompressionMiddleware, add_content_coding
from core.compression.negotiation import ContentNegotiationMiddleware
from core.request_limits import MaxBodySizeMiddleware
from core.zstd_compat import zstd

LIMIT = 4096


def payload(n: int) -> bytes:
    """A JSON document of exactly n bytes (n >= 8)."""
    return b'{"x":"' + b"a" * (n - 8) + b'"}'


async def sized(request: Request) -> Response:
    return Response(payload(request.path_params["n"]), media_type="application/json")


async def streamed(_: Request) -> Response:
    async def chunks():
        for _ in range(4):
            yield payload(1000)

    return StreamingResponse(chunks(), media_type="application/json")


async def sse(_: Request) -> Response:
    async def events():
        yield b"data: " + b"a" * 2000 + b"\n\n"

    return StreamingResponse(events(), media_type="text/event-stream")


async def binary(_: Request) -> Response:
    return Response(b"\0" * 4096, media_type="application/octet-stream")


async def with_vary(_: Request) -> Response:
    return Response(
        payload(2000), media_type="application/json", headers={"Vary": "Origin"}
    )


async def already_varies(_: Request) -> Response:
    return Response(
        payload(2000),
        media_type="application/json",
        headers={"Vary": "Origin, accept-encoding"},
    )


async def partial(_: Request) -> Response:
    return Response(
        payload(2000),
        status_code=206,
        media_type="text/plain",
        headers={"Content-Range": "bytes 0-1999/4000"},
    )


async def ranged(_: Request) -> Response:
    # Status 200, so only the Content-Range check keeps it uncompressed.
    return Response(
        payload(2000),
        media_type="text/plain",
        headers={"Content-Range": "bytes 0-1999/4000"},
    )


async def vary_star(_: Request) -> Response:
    return Response(payload(2000), media_type="application/json", headers={"Vary": "*"})


async def small(_: Request) -> Response:
    return JSONResponse({"ok": True})


async def status(request: Request) -> Response:
    # A body over the threshold, so only the status check keeps it uncompressed.
    return Response(
        payload(2000),
        status_code=request.path_params["code"],
        media_type="application/json",
    )


async def pre_encoded(_: Request) -> Response:
    return Response(
        gzip.compress(payload(2000)),
        media_type="application/json",
        headers={"Content-Encoding": "gzip"},
    )


async def empty(_: Request) -> Response:
    return Response(b"", media_type="application/json")


async def echo(request: Request) -> Response:
    body = await request.body()
    return Response(
        body,
        media_type="application/octet-stream",
        headers={"x-seen-encoding": request.headers.get("content-encoding", "")},
    )


def make_app(**options: Any) -> Starlette:
    app = Starlette(
        routes=[
            Route("/sized/{n:int}", sized),
            Route("/streamed", streamed),
            Route("/sse", sse),
            Route("/binary", binary),
            Route("/vary", with_vary),
            Route("/already-varies", already_varies),
            Route("/partial", partial),
            Route("/ranged", ranged),
            Route("/vary-star", vary_star),
            Route("/status/{code:int}", status),
            Route("/pre-encoded", pre_encoded),
            Route("/empty", empty),
            Route("/echo", echo, methods=["POST"]),
            Route("/data", small),
            Route("/health/live", small),
            Route("/health/ready", small),
            Route("/metrics", small),
            Route("/mcp/", small, methods=["GET", "POST"]),
        ]
    )
    app.add_middleware(MaxBodySizeMiddleware, max_bytes=LIMIT)
    add_content_coding(app, max_body_bytes=LIMIT, **options)
    return app


async def send(
    app: Starlette,
    path: str,
    *,
    method: str = "GET",
    accept: str | None = None,
    headers: dict[str, str] | None = None,
    content: bytes | None = None,
) -> tuple[HttpxResponse, bytes]:
    """`accept=None` sends no Accept-Encoding at all (httpx adds one by default)."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        del c.headers["accept-encoding"]
        request_headers = dict(headers or {})
        if accept is not None:
            request_headers["accept-encoding"] = accept
        async with c.stream(
            method, path, headers=request_headers, content=content
        ) as r:
            raw = b"".join([chunk async for chunk in r.aiter_raw()])
    return r, raw


def vary(r: HttpxResponse) -> list[str]:
    return [
        v.strip().lower() for v in r.headers.get("vary", "").split(",") if v.strip()
    ]


async def test_zstd_is_chosen_and_decodes_to_the_original() -> None:
    r, raw = await send(make_app(), "/sized/2000", accept="gzip, zstd")
    assert r.headers["content-encoding"] == "zstd"
    assert zstd.decompress(raw) == payload(2000)
    assert int(r.headers["content-length"]) == len(raw)
    assert "accept-encoding" in vary(r)


async def test_gzip_when_only_gzip_is_accepted() -> None:
    r, raw = await send(make_app(), "/sized/2000", accept="gzip")
    assert r.headers["content-encoding"] == "gzip"
    assert gzip.decompress(raw) == payload(2000)


async def test_identity_and_missing_header_are_uncompressed_but_vary() -> None:
    for accept in ("identity", None):
        r, raw = await send(make_app(), "/sized/2000", accept=accept)
        assert "content-encoding" not in r.headers
        assert raw == payload(2000)
        assert "accept-encoding" in vary(r)


async def test_threshold_boundary() -> None:
    app = make_app(min_bytes=1024)
    below, raw_below = await send(app, "/sized/1023", accept="zstd")
    at, raw_at = await send(app, "/sized/1024", accept="zstd")
    assert "content-encoding" not in below.headers and raw_below == payload(1023)
    assert at.headers["content-encoding"] == "zstd"
    assert zstd.decompress(raw_at) == payload(1024)


async def test_streamed_body_is_compressed_without_content_length() -> None:
    r, raw = await send(make_app(), "/streamed", accept="zstd")
    assert r.headers["content-encoding"] == "zstd"
    assert "content-length" not in r.headers
    assert zstd.decompress(raw) == payload(1000) * 4


async def test_head_sse_and_binary_are_never_compressed() -> None:
    head, _ = await send(make_app(), "/sized/2000", method="HEAD", accept="zstd")
    stream, stream_raw = await send(make_app(), "/sse", accept="zstd")
    blob, blob_raw = await send(make_app(), "/binary", accept="zstd")
    assert "content-encoding" not in head.headers
    assert "content-encoding" not in stream.headers and stream_raw.startswith(b"data: ")
    assert "content-encoding" not in blob.headers and blob_raw == b"\0" * 4096


async def test_204_and_304_are_never_compressed() -> None:
    for app in (make_app(), make_app(require_zstd=True)):
        for code in (204, 304):
            r, raw = await send(app, f"/status/{code}", accept="zstd")
            assert r.status_code == code
            assert "content-encoding" not in r.headers, (code, app)
            assert raw == payload(2000)


async def test_already_encoded_response_is_not_compressed_again() -> None:
    for app in (make_app(), make_app(require_zstd=True)):
        r, raw = await send(app, "/pre-encoded", accept="zstd")
        assert r.headers["content-encoding"] == "gzip"
        assert gzip.decompress(raw) == payload(2000)


async def test_strict_mode_sends_an_empty_body_unchanged() -> None:
    r, raw = await send(make_app(require_zstd=True), "/empty", accept="zstd")
    assert r.status_code == 200
    assert "content-encoding" not in r.headers and raw == b""
    assert "accept-encoding" in vary(r)


async def test_vary_is_merged_with_an_existing_value() -> None:
    r, _ = await send(make_app(), "/vary", accept="zstd")
    assert vary(r) == ["origin", "accept-encoding"]


async def test_vary_already_naming_accept_encoding_is_not_repeated() -> None:
    r, _ = await send(make_app(), "/already-varies", accept="zstd")
    assert vary(r) == ["origin", "accept-encoding"]


async def test_partial_content_is_never_compressed() -> None:
    r, raw = await send(make_app(), "/partial", accept="zstd")
    assert r.status_code == 206
    assert "content-encoding" not in r.headers
    assert raw == payload(2000)


async def call_raw(messages: list[Message]) -> list[Message]:
    """Drive the middleware directly with an app that sends `messages`; returns
    what reaches the server's send."""

    async def app(_: Scope, __: Receive, send: Send) -> None:
        for message in messages:
            await send(message)

    async def receive() -> Message:
        return {"type": "http.disconnect"}

    sent: list[Message] = []

    async def record(message: Message) -> None:
        sent.append(message)

    scope = {
        "type": "http",
        "method": "GET",
        "path": "/file",
        "headers": [(b"accept-encoding", b"zstd")],
    }
    await ResponseCompressionMiddleware(app)(scope, receive, record)
    return sent


TEXT_START: Message = {
    "type": "http.response.start",
    "status": 200,
    "headers": [(b"content-type", b"text/plain")],
}


async def test_content_range_alone_prevents_compression() -> None:
    r, raw = await send(make_app(), "/ranged", accept="zstd")
    assert r.status_code == 200
    assert "content-encoding" not in r.headers
    assert raw == payload(2000)


async def test_vary_star_is_left_unchanged() -> None:
    r, _ = await send(make_app(), "/vary-star", accept="zstd")
    assert r.headers.get_list("vary") == ["*"]


async def test_held_start_goes_out_before_a_non_body_message() -> None:
    sent = await call_raw(
        [TEXT_START, {"type": "http.response.pathsend", "path": "/srv/big.txt"}]
    )
    assert [m["type"] for m in sent] == [
        "http.response.start",
        "http.response.pathsend",
    ]
    headers = dict(sent[0]["headers"])
    assert b"content-encoding" not in headers
    assert headers[b"vary"] == b"Accept-Encoding"


async def test_trailers_after_a_small_body_do_not_repeat_the_start() -> None:
    sent = await call_raw(
        [
            TEXT_START,
            {"type": "http.response.body", "body": b"short"},
            {"type": "http.response.trailers", "headers": [], "more_trailers": False},
        ]
    )
    assert [m["type"] for m in sent] == [
        "http.response.start",
        "http.response.body",
        "http.response.trailers",
    ]


async def test_body_held_below_the_threshold_goes_out_before_pathsend() -> None:
    pathsend: Message = {"type": "http.response.pathsend", "path": "/srv/big.txt"}
    sent = await call_raw(
        [
            TEXT_START,
            {"type": "http.response.body", "body": b"abc", "more_body": True},
            pathsend,
        ]
    )
    assert [m["type"] for m in sent] == [
        "http.response.start",
        "http.response.body",
        "http.response.pathsend",
    ]
    assert sent[1]["body"] == b"abc" and sent[1]["more_body"] is True


async def test_empty_held_body_is_not_flushed_before_pathsend() -> None:
    sent = await call_raw(
        [
            TEXT_START,
            {"type": "http.response.body", "body": b"", "more_body": True},
            {"type": "http.response.pathsend", "path": "/srv/big.txt"},
        ]
    )
    assert [m["type"] for m in sent] == [
        "http.response.start",
        "http.response.pathsend",
    ]


async def test_trailers_follow_a_compressed_stream_without_a_second_start() -> None:
    sent = await call_raw(
        [
            TEXT_START,
            {"type": "http.response.body", "body": payload(2000), "more_body": True},
            {"type": "http.response.body", "body": b""},
            {"type": "http.response.trailers", "headers": [], "more_trailers": False},
        ]
    )
    types = [m["type"] for m in sent]
    assert types.count("http.response.start") == 1
    assert types[-1] == "http.response.trailers"
    assert dict(sent[0]["headers"])[b"content-encoding"] == b"zstd"
    body = b"".join(m["body"] for m in sent if m["type"] == "http.response.body")
    assert zstd.decompress(body) == payload(2000)


async def test_disabled_sends_identity() -> None:
    r, raw = await send(make_app(enabled=False), "/sized/2000", accept="zstd")
    assert "content-encoding" not in r.headers and raw == payload(2000)


async def test_strict_mode_compresses_below_the_threshold() -> None:
    r, raw = await send(make_app(require_zstd=True), "/sized/20", accept="zstd")
    assert r.headers["content-encoding"] == "zstd"
    assert zstd.decompress(raw) == payload(20)


async def test_strict_mode_refuses_clients_without_zstd() -> None:
    app = make_app(require_zstd=True)
    for accept in (None, "identity", "gzip", "zstd;q=0"):
        r, raw = await send(app, "/data", accept=accept)
        assert r.status_code == 406, accept
        assert r.headers["content-type"] == "application/json"
        assert "content-encoding" not in r.headers
        assert vary(r) == ["accept-encoding"]
        assert raw == b'{"detail": "Accept-Encoding must allow zstd"}'


async def test_strict_mode_exempts_probes_metrics_mcp_and_preflight() -> None:
    app = make_app(require_zstd=True)
    for path in ("/health/live", "/health/ready", "/metrics", "/mcp/"):
        r, _ = await send(app, path, accept="identity")
        assert r.status_code == 200, path
    preflight, _ = await send(app, "/data", method="OPTIONS", accept=None)
    assert preflight.status_code != 406


BODY = b"hello world " * 100


async def test_zstd_and_gzip_bodies_reach_the_app_decompressed() -> None:
    for encoding, data in (
        ("zstd", zstd.compress(BODY)),
        ("gzip", gzip.compress(BODY)),
        (" ZSTD ", zstd.compress(BODY)),
    ):
        r, raw = await send(
            make_app(),
            "/echo",
            method="POST",
            headers={"content-encoding": encoding},
            content=data,
        )
        assert r.status_code == 200, encoding
        assert raw == BODY
        assert r.headers["x-seen-encoding"] == ""


async def test_identity_and_absent_content_encoding_pass_through() -> None:
    for headers in ({"content-encoding": "identity"}, {}):
        r, raw = await send(
            make_app(), "/echo", method="POST", headers=headers, content=BODY
        )
        assert r.status_code == 200 and raw == BODY


async def test_unsupported_or_stacked_coding_is_415() -> None:
    for encoding in ("br", "deflate", "gzip, zstd"):
        r, raw = await send(
            make_app(),
            "/echo",
            method="POST",
            headers={"content-encoding": encoding},
            content=BODY,
        )
        assert r.status_code == 415, encoding
        assert r.headers["accept-encoding"] == "zstd, gzip"
        assert vary(r) == ["accept-encoding"]
        assert raw == b'{"detail": "Unsupported Content-Encoding"}'


async def test_undecodable_bodies_are_400() -> None:
    frame = zstd.compress(BODY)
    for encoding, data in (
        ("zstd", b"not zstd at all"),
        ("zstd", frame[:-4]),  # truncated
        ("zstd", frame + b"trailing"),
        ("gzip", b"not gzip"),
        ("zstd", b""),
    ):
        r, raw = await send(
            make_app(),
            "/echo",
            method="POST",
            headers={"content-encoding": encoding},
            content=data,
        )
        assert r.status_code == 400, data[:12]
        assert raw == b'{"detail": "Malformed compressed request body"}'
        assert vary(r) == ["accept-encoding"]


async def test_decompression_bomb_is_413_without_full_expansion() -> None:
    bomb = zstd.compress(b"\0" * (LIMIT * 1000))  # 4 MB of zeros in a few hundred bytes
    assert len(bomb) < LIMIT
    r, _ = await send(
        make_app(),
        "/echo",
        method="POST",
        headers={"content-encoding": "zstd"},
        content=bomb,
    )
    assert r.status_code == 413


async def test_compressed_body_over_the_limit_is_413() -> None:
    incompressible = zstd.compress(os.urandom(LIMIT * 2))
    r, _ = await send(
        make_app(),
        "/echo",
        method="POST",
        headers={"content-encoding": "zstd"},
        content=incompressible,
    )
    assert r.status_code == 413
    assert vary(r) == ["accept-encoding"]


async def test_disabled_errors_do_not_vary() -> None:
    for headers, content, code in (
        ({"content-encoding": "br"}, BODY, 415),
        ({"content-encoding": "zstd"}, b"not zstd", 400),
        ({"content-encoding": "zstd"}, os.urandom(LIMIT * 2), 413),
    ):
        r, _ = await send(
            make_app(enabled=False),
            "/echo",
            method="POST",
            headers=headers,
            content=content,
        )
        assert r.status_code == code
        assert "vary" not in r.headers


async def test_the_app_sees_only_the_winning_coding() -> None:
    """Negotiation rewrites Accept-Encoding to the one coding the response
    layer will use (zstd wins ties; a `*` entry counts), or removes it."""
    seen: list[str | None] = []

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        seen.append(Headers(scope=scope).get("accept-encoding"))
        await Response(b"ok")(scope, receive, send)

    negotiation = ContentNegotiationMiddleware(app)
    async with AsyncClient(
        transport=ASGITransport(app=negotiation), base_url="http://t"
    ) as c:
        for accept in ("gzip;q=0.5, zstd", "gzip, zstd;q=0.1", "*", "identity"):
            await c.get("/x", headers={"accept-encoding": accept})
    assert seen == ["zstd", "gzip", "zstd", None]


async def test_strict_mode_compresses_request_decoding_errors_like_any_response() -> (
    None
):
    """Request decoding sits inside the response layer, so its JSON errors
    are compressed in strict mode like any other response."""
    r, raw = await send(
        make_app(require_zstd=True),
        "/echo",
        method="POST",
        accept="zstd",
        headers={"content-encoding": "zstd"},
        content=b"not zstd",
    )
    assert r.status_code == 400
    assert r.headers["content-encoding"] == "zstd"
    assert vary(r) == ["accept-encoding"]
    assert json.loads(zstd.decompress(raw)) == {
        "detail": "Malformed compressed request body"
    }
