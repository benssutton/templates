"""zstd/gzip content coding, negotiated from Accept-Encoding (RFC 9110).

Responses with a text-based content type are compressed with the coding the
client prefers (zstd wins ties) once the body reaches min_bytes; strict mode
compresses every such response and answers 406 to a client that does not
accept zstd, except on the probe, metrics and MCP routes, which orchestrators,
scrapers and MCP SDKs call without zstd support.

Must sit outside MaxBodySizeMiddleware: request bodies are decompressed here,
so the body limit counts decompressed bytes.
"""

import json
import zlib
from collections.abc import Iterable

from starlette.datastructures import Headers, MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from core.content_coding import Coding, accepts_zstd, choose
from core.zstd_compat import zstd

_EXEMPT = frozenset({"/health/live", "/health/ready", "/metrics", "/mcp"})
_TEXT_TYPES = frozenset({"application/json", "application/xml"})


def _exempt(path: str) -> bool:
    return path in _EXEMPT or path.startswith("/mcp/")


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


class _Encoder:
    """Streaming compressor; chunk() flushes so a streamed response is never held back."""

    def __init__(self, coding: Coding, zstd_level: int) -> None:
        if coding == "zstd":
            self._zstd = zstd.ZstdCompressor(level=zstd_level)
            self._gzip = None
        else:
            self._zstd = None
            self._gzip = zlib.compressobj(wbits=31)  # 31: gzip framing

    def chunk(self, data: bytes) -> bytes:
        if self._zstd is not None:
            return self._zstd.compress(data) + self._zstd.flush(
                zstd.ZstdCompressor.FLUSH_BLOCK
            )
        assert self._gzip is not None
        return self._gzip.compress(data) + self._gzip.flush(zlib.Z_SYNC_FLUSH)

    def last(self, data: bytes) -> bytes:
        if self._zstd is not None:
            return self._zstd.compress(data) + self._zstd.flush(
                zstd.ZstdCompressor.FLUSH_FRAME
            )
        assert self._gzip is not None
        return self._gzip.compress(data) + self._gzip.flush()


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
        self.min_bytes = 0 if require_zstd else min_bytes
        self.zstd_level = zstd_level
        self.require_zstd = require_zstd

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        accept = Headers(scope=scope).get("accept-encoding")
        if (
            self.require_zstd
            and scope["method"] != "OPTIONS"
            and not _exempt(scope["path"])
            and not accepts_zstd(accept)
        ):
            await _send_error(send, 406, "Accept-Encoding must allow zstd")
            return
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
                headers = MutableHeaders(raw=list(message["headers"]))
                if not _compressible(headers.get("content-type", "")):
                    passthrough = True
                    await send(message)
                    return
                headers.add_vary_header("Accept-Encoding")
                message = {**message, "headers": headers.raw}
                if (
                    coding is None
                    or scope["method"] == "HEAD"
                    or message["status"] in (204, 304)
                    or "content-encoding" in headers
                ):
                    passthrough = True
                    await send(message)
                    return
                start = message
                return
            if passthrough or start is None or message["type"] != "http.response.body":
                await send(message)
                return
            body: bytes = message.get("body", b"")
            more: bool = message.get("more_body", False)
            if encoder is None:
                buffered.append(body)
                size += len(body)
                if size < self.min_bytes:
                    if not more:
                        await send(start)
                        await send(
                            {"type": "http.response.body", "body": b"".join(buffered)}
                        )
                    return
                assert coding is not None
                encoder = _Encoder(coding, self.zstd_level)
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
