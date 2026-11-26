"""Compresses text-based responses with the coding ContentNegotiationMiddleware
chose (the request's rewritten Accept-Encoding), and adds
`Vary: Accept-Encoding` to every text-based response, compressed or not.

A response is held until its body reaches min_bytes or ends, so a small one
goes out uncompressed. Past that it streams, each chunk flushed so a streamed
response is never held back. Event streams, HEAD, 204/206/304, ranged and
already-encoded responses are never compressed. Compression runs inline on
the event loop, bounded by min_bytes.
"""

import zlib
from dataclasses import dataclass, field
from typing import Protocol

from starlette.datastructures import Headers, MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from core.content_coding import Coding, choose
from core.zstd_compat import zstd

_TEXT_TYPES = frozenset({"application/json", "application/xml"})


def _compressible(content_type: str) -> bool:
    media = content_type.split(";", 1)[0].strip().lower()
    if media == "text/event-stream":
        return False  # compressing would buffer events
    return (
        media.startswith("text/")
        or media in _TEXT_TYPES
        or media.endswith(("+json", "+xml"))
    )


def _varies_on_accept_encoding(headers: MutableHeaders) -> bool:
    values = {v.strip().lower() for v in ",".join(headers.getlist("vary")).split(",")}
    return "accept-encoding" in values or "*" in values


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


@dataclass
class _AwaitingStart:
    """No http.response.start yet."""


@dataclass
class _Passthrough:
    """Not compressed: every message goes out unchanged."""


@dataclass
class _Holding:
    """Start held while the body is buffered up to min_bytes."""

    start: Message
    coding: Coding
    chunks: list[bytes] = field(default_factory=list)
    size: int = 0


@dataclass
class _Streaming:
    """Start sent with Content-Encoding; every body chunk is encoded."""

    encoder: _Encoder


_State = _AwaitingStart | _Passthrough | _Holding | _Streaming


class _CompressingSend:
    """The send() of one response:

    AwaitingStart --start, not to be compressed--> Passthrough
    AwaitingStart --start, compressible-----------> Holding
    Holding --body reaches min_bytes--------------> Streaming
    Holding --body ends below min_bytes, or a non-body message--> Passthrough
    """

    def __init__(
        self,
        send: Send,
        coding: Coding | None,
        *,
        head: bool,
        min_bytes: int,
        zstd_level: int,
    ) -> None:
        self._send = send
        self._coding: Coding | None = coding
        self._head = head
        self._min_bytes = min_bytes
        self._zstd_level = zstd_level
        self._state: _State = _AwaitingStart()

    async def __call__(self, message: Message) -> None:
        match self._state:
            case _AwaitingStart() if message["type"] == "http.response.start":
                await self._on_start(message)
            case _Holding() as held:
                await self._on_held(held, message)
            case _Streaming(encoder) if message["type"] == "http.response.body":
                await self._on_streaming(encoder, message)
            case _:
                await self._send(message)

    async def _on_start(self, message: Message) -> None:
        headers = MutableHeaders(raw=list(message.get("headers", [])))
        if not _compressible(headers.get("content-type", "")):
            self._state = _Passthrough()
            await self._send(message)
            return
        if not _varies_on_accept_encoding(headers):
            headers.add_vary_header("Accept-Encoding")
        message = {**message, "headers": headers.raw}
        if (
            self._coding is None
            or self._head
            or message["status"] in (204, 206, 304)
            or "content-encoding" in headers
            or "content-range" in headers
        ):
            self._state = _Passthrough()
            await self._send(message)
            return
        self._state = _Holding(start=message, coding=self._coding)

    async def _on_held(self, held: _Holding, message: Message) -> None:
        if message["type"] != "http.response.body":
            # pathsend, zerocopysend, debug (trailers only follow a final
            # body): the held start, and any body held with it, go out first,
            # uncompressed.
            self._state = _Passthrough()
            await self._send(held.start)
            if held.size:
                await self._send(
                    {
                        "type": "http.response.body",
                        "body": b"".join(held.chunks),
                        "more_body": True,
                    }
                )
            await self._send(message)
            return
        body: bytes = message.get("body", b"")
        more: bool = message.get("more_body", False)
        held.chunks.append(body)
        held.size += len(body)
        # Nothing to compress yet; an empty body stays empty even in strict mode.
        if held.size < self._min_bytes or held.size == 0:
            if not more:
                self._state = _Passthrough()
                await self._send(held.start)
                await self._send(
                    {"type": "http.response.body", "body": b"".join(held.chunks)}
                )
            return
        encoder = _encoder(held.coding, self._zstd_level)
        self._state = _Streaming(encoder)
        # Edits the held start's header list in place.
        headers = MutableHeaders(raw=held.start["headers"])
        headers["content-encoding"] = held.coding
        del headers["content-length"]
        body = b"".join(held.chunks)
        if more:
            await self._send(held.start)
            await self._send(
                {
                    "type": "http.response.body",
                    "body": encoder.chunk(body),
                    "more_body": True,
                }
            )
            return
        out = encoder.last(body)
        headers["content-length"] = str(len(out))
        await self._send(held.start)
        await self._send({"type": "http.response.body", "body": out})

    async def _on_streaming(self, encoder: _Encoder, message: Message) -> None:
        body: bytes = message.get("body", b"")
        if message.get("more_body", False):
            await self._send(
                {
                    "type": "http.response.body",
                    "body": encoder.chunk(body),
                    "more_body": True,
                }
            )
        else:
            await self._send({"type": "http.response.body", "body": encoder.last(body)})


class ResponseCompressionMiddleware:
    def __init__(
        self, app: ASGIApp, *, min_bytes: int = 1024, zstd_level: int = 3
    ) -> None:
        self.app = app
        self.min_bytes = min_bytes
        self.zstd_level = zstd_level

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        compressing = _CompressingSend(
            send,
            choose(Headers(scope=scope).get("accept-encoding")),
            head=scope["method"] == "HEAD",
            min_bytes=self.min_bytes,
            zstd_level=self.zstd_level,
        )
        await self.app(scope, receive, compressing)
