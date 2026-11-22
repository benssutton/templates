"""zstd/gzip content coding, negotiated from Accept-Encoding (RFC 9110).

Responses with a text-based content type are compressed with the coding the
client prefers (zstd wins ties) once the body reaches min_bytes; strict mode
compresses every such response and answers 406 to a client that does not
accept zstd, except on the probe, metrics and MCP routes, which orchestrators,
scrapers and MCP SDKs call without zstd support.

A streamed text response is buffered until it reaches min_bytes or ends
(event streams are exempt). Compression and decompression run inline on the
event loop, bounded by min_bytes and the request body limit.

Must sit outside MaxBodySizeMiddleware: request bodies are decompressed here,
so the body limit counts decompressed bytes. The 406/415/400/413 answered here
sit outside the correlation-id, CORS and Server-Timing layers (as the body
limit's own 413 does), so they carry none of those headers.
"""

import json
import zlib
from collections.abc import Iterable
from typing import Protocol

from starlette.datastructures import Headers, MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from core.content_coding import Coding, accepts_zstd, choose
from core.zstd_compat import zstd

_EXEMPT = frozenset({"/health/live", "/health/ready", "/metrics", "/mcp"})
_TEXT_TYPES = frozenset({"application/json", "application/xml"})


def _exempt(path: str) -> bool:
    return path in _EXEMPT or path.startswith("/mcp/")


def _varies_on_accept_encoding(headers: MutableHeaders) -> bool:
    values = {v.strip().lower() for v in ",".join(headers.getlist("vary")).split(",")}
    return "accept-encoding" in values or "*" in values


def _compressible(content_type: str) -> bool:
    media = content_type.split(";", 1)[0].strip().lower()
    if media == "text/event-stream":
        return False  # compressing would buffer events
    return (
        media.startswith("text/")
        or media in _TEXT_TYPES
        or media.endswith(("+json", "+xml"))
    )


async def _send_error(
    send: Send, status: int, detail: str, extra: Iterable[tuple[bytes, bytes]] = ()
) -> None:
    body = json.dumps({"detail": detail}).encode()
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode()),
                *extra,
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})


_PAYLOAD_TOO_LARGE = "Payload Too Large"


class _MalformedBody(Exception):
    pass


def _decompress(coding: str, data: bytes, limit: int) -> bytes:
    """At most limit + 1 bytes: one byte over is enough for the body limit to
    refuse it, so a decompression bomb is never fully expanded. A single
    frame/member only: trailing data is treated as malformed."""
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


class _Encoder(Protocol):
    """Streaming compressor; chunk() flushes so a streamed response is never held back."""

    def chunk(self, data: bytes) -> bytes: ...

    def last(self, data: bytes) -> bytes: ...


class _ZstdEncoder:
    def __init__(self, level: int) -> None:
        self._c = zstd.ZstdCompressor(level=level)

    def chunk(self, data: bytes) -> bytes:
        return self._c.compress(data) + self._c.flush(zstd.ZstdCompressor.FLUSH_BLOCK)

    def last(self, data: bytes) -> bytes:
        return self._c.compress(data) + self._c.flush(zstd.ZstdCompressor.FLUSH_FRAME)


class _GzipEncoder:
    def __init__(self) -> None:
        self._c = zlib.compressobj(wbits=31)  # 31: gzip framing

    def chunk(self, data: bytes) -> bytes:
        return self._c.compress(data) + self._c.flush(zlib.Z_SYNC_FLUSH)

    def last(self, data: bytes) -> bytes:
        return self._c.compress(data) + self._c.flush()


def _encoder(coding: Coding, zstd_level: int) -> _Encoder:
    return _ZstdEncoder(zstd_level) if coding == "zstd" else _GzipEncoder()


class CompressionMiddleware:
    def __init__(
        self,
        app: ASGIApp,
        *,
        max_body_bytes: int,
        enabled: bool = True,
        min_bytes: int = 1024,
        zstd_level: int = 3,
        require_zstd: bool = False,
    ) -> None:
        self.app = app
        self.max_body_bytes = max_body_bytes
        self.enabled = enabled
        # Strict mode compresses at any size; empty bodies are still left alone.
        self.min_bytes = 0 if require_zstd else min_bytes
        self.zstd_level = zstd_level
        self.require_zstd = require_zstd

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        request_headers = Headers(scope=scope)
        accept = request_headers.get("accept-encoding")
        if (
            self.require_zstd
            and scope["method"] != "OPTIONS"
            and not _exempt(scope["path"])
            and not accepts_zstd(accept)
        ):
            await _send_error(
                send,
                406,
                "Accept-Encoding must allow zstd",
                [(b"vary", b"Accept-Encoding")],
            )
            return
        encoding = request_headers.get("content-encoding", "").strip().lower()
        if encoding not in ("", "identity"):
            if encoding not in ("zstd", "gzip"):
                await _send_error(
                    send,
                    415,
                    "Unsupported Content-Encoding",
                    [(b"accept-encoding", b"zstd, gzip")],
                )
                return
            compressed = await _read(receive, self.max_body_bytes)
            if compressed is None:
                await _send_error(send, 413, _PAYLOAD_TOO_LARGE)
                return
            try:
                body = _decompress(encoding, compressed, self.max_body_bytes)
            except _MalformedBody:
                await _send_error(send, 400, "Malformed compressed request body")
                return
            # An over-limit result keeps one extra byte; MaxBodySizeMiddleware,
            # inside this one, answers its 413 from the Content-Length.
            scope = _decoded_scope(scope, len(body))
            receive = _replay(body, receive)
        if not self.enabled:
            await self.app(scope, receive, send)
            return
        await self.app(
            scope, receive, self._compressing_send(scope, send, choose(accept))
        )

    def _compressing_send(
        self, scope: Scope, send: Send, coding: Coding | None
    ) -> Send:
        start: Message | None = None
        passthrough = False
        encoder: _Encoder | None = None
        buffered: list[bytes] = []
        size = 0

        async def wrapped(message: Message) -> None:
            nonlocal start, passthrough, encoder, size
            if message["type"] == "http.response.start":
                headers = MutableHeaders(raw=list(message.get("headers", [])))
                if not _compressible(headers.get("content-type", "")):
                    passthrough = True
                    await send(message)
                    return
                if not _varies_on_accept_encoding(headers):
                    headers.add_vary_header("Accept-Encoding")
                message = {**message, "headers": headers.raw}
                if (
                    coding is None
                    or scope["method"] == "HEAD"
                    or message["status"] in (204, 206, 304)
                    or "content-encoding" in headers
                    or "content-range" in headers
                ):
                    passthrough = True
                    await send(message)
                    return
                start = message
                return
            if passthrough or start is None:
                await send(message)
                return
            if message["type"] != "http.response.body":
                if encoder is None:
                    # pathsend, zerocopysend, debug (trailers only follow a final
                    # body): the held start, and any body held with it, must go
                    # out first, uncompressed.
                    passthrough = True
                    await send(start)
                    if size:
                        await send(
                            {
                                "type": "http.response.body",
                                "body": b"".join(buffered),
                                "more_body": True,
                            }
                        )
                await send(message)
                return
            body: bytes = message.get("body", b"")
            more: bool = message.get("more_body", False)
            if encoder is None:
                buffered.append(body)
                size += len(body)
                # Nothing to compress yet; an empty body stays empty even in strict mode.
                if size < self.min_bytes or size == 0:
                    if not more:
                        passthrough = True
                        await send(start)
                        await send(
                            {"type": "http.response.body", "body": b"".join(buffered)}
                        )
                    return
                assert coding is not None
                encoder = _encoder(coding, self.zstd_level)
                headers = MutableHeaders(raw=start["headers"])
                headers["content-encoding"] = coding
                del headers["content-length"]
                body = b"".join(buffered)
                buffered.clear()
                if not more:
                    out = encoder.last(body)
                    headers["content-length"] = str(len(out))
                    await send(start)
                    await send({"type": "http.response.body", "body": out})
                    return
                await send(start)
            if more:
                await send(
                    {
                        "type": "http.response.body",
                        "body": encoder.chunk(body),
                        "more_body": True,
                    }
                )
            else:
                await send({"type": "http.response.body", "body": encoder.last(body)})

        return wrapped
