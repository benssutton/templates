"""Decodes zstd and gzip request bodies before the app reads them.

At most limit + 1 bytes are expanded: one byte over is enough for
MaxBodySizeMiddleware, which sits inside this layer, to answer 413 from the
rewritten Content-Length. So a decompression bomb is never fully expanded and
the body limit counts decompressed bytes. A body that does not decode is 400.
Only one zstd frame or gzip member is accepted: trailing data is malformed.
ContentNegotiationMiddleware has already refused every other coding.
"""

import zlib

from starlette.datastructures import Headers
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from core.compression._json_error import send_json_error
from core.zstd_compat import zstd


class _MalformedBody(Exception):
    pass


def _decompress(coding: str, data: bytes, limit: int) -> bytes:
    """At most limit + 1 bytes; a single frame/member only."""
    try:
        if coding == "zstd":
            d = zstd.ZstdDecompressor()
            out = d.decompress(data, max_length=limit + 1)
            done, rest = d.eof, d.unused_data
        else:
            g = zlib.decompressobj(wbits=31)
            out = g.decompress(data, limit + 1)
            done, rest = g.eof, g.unused_data
    except (zstd.ZstdError, zlib.error) as exc:
        raise _MalformedBody from exc
    if len(out) > limit:
        return out
    if not done or rest:
        raise _MalformedBody
    return out


async def _read(receive: Receive, limit: int) -> bytes | None:
    """The whole request body, or None once it passes `limit` bytes."""
    chunks: list[bytes] = []
    size = 0
    while True:
        message = await receive()
        if message["type"] != "http.request":
            return b"".join(chunks)  # disconnect: the app sees a short body
        chunk = message.get("body", b"")
        size += len(chunk)
        if size > limit:
            return None
        chunks.append(chunk)
        if not message.get("more_body", False):
            return b"".join(chunks)


def _replay(body: bytes, receive: Receive) -> Receive:
    sent = False

    async def replay() -> Message:
        nonlocal sent
        if sent:
            return await receive()  # later calls wait for http.disconnect
        sent = True
        return {"type": "http.request", "body": body, "more_body": False}

    return replay


def _decoded_scope(scope: Scope, length: int) -> Scope:
    headers = [
        (k, v)
        for k, v in scope["headers"]
        if k not in (b"content-encoding", b"content-length", b"transfer-encoding")
    ]
    headers.append((b"content-length", str(length).encode()))
    return {**scope, "headers": headers}


class RequestDecompressionMiddleware:
    def __init__(self, app: ASGIApp, *, max_body_bytes: int) -> None:
        self.app = app
        self.max_body_bytes = max_body_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        coding = Headers(scope=scope).get("content-encoding", "").strip().lower()
        if coding not in ("zstd", "gzip"):
            await self.app(scope, receive, send)
            return
        compressed = await _read(receive, self.max_body_bytes)
        if compressed is None:
            await send_json_error(send, 413, "Payload Too Large")
            return
        try:
            body = _decompress(coding, compressed, self.max_body_bytes)
        except _MalformedBody:
            await send_json_error(send, 400, "Malformed compressed request body")
            return
        await self.app(
            _decoded_scope(scope, len(body)), _replay(body, receive), send
        )
