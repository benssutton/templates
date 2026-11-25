"""Request-side content-coding policy, outermost of the three layers.

Strict mode answers 406 to a client whose Accept-Encoding does not allow zstd,
except on the probe, metrics and MCP routes (orchestrators, scrapers and MCP
SDKs call them without zstd support) and on CORS preflight. A request body in
a coding other than zstd or gzip is refused with 415. Every other request goes
on with its Accept-Encoding rewritten to the single coding the client prefers
(zstd wins ties), or removed when it accepts neither, so the response layer
only has to read that header.
"""

from starlette.datastructures import Headers
from starlette.types import ASGIApp, Receive, Scope, Send

from core.compression._json_error import send_json_error
from core.content_coding import accepts_zstd, choose

_EXEMPT = frozenset({"/health/live", "/health/ready", "/metrics", "/mcp"})
_VARY = (b"vary", b"Accept-Encoding")


def _exempt(path: str) -> bool:
    return path in _EXEMPT or path.startswith("/mcp/")


def _with_accept_encoding(scope: Scope, coding: str | None) -> Scope:
    headers = [(k, v) for k, v in scope["headers"] if k != b"accept-encoding"]
    if coding is not None:
        headers.append((b"accept-encoding", coding.encode()))
    return {**scope, "headers": headers}


class ContentNegotiationMiddleware:
    def __init__(
        self, app: ASGIApp, *, enabled: bool = True, require_zstd: bool = False
    ) -> None:
        self.app = app
        self.enabled = enabled
        self.require_zstd = require_zstd

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = Headers(scope=scope)
        accept = headers.get("accept-encoding")
        if (
            self.require_zstd
            and scope["method"] != "OPTIONS"
            and not _exempt(scope["path"])
            and not accepts_zstd(accept)
        ):
            await send_json_error(send, 406, "Accept-Encoding must allow zstd", [_VARY])
            return
        encoding = headers.get("content-encoding", "").strip().lower()
        if encoding not in ("", "identity", "zstd", "gzip"):
            # JSON, so while compression is on a cache must key it on
            # Accept-Encoding like any other compressible response.
            vary = [_VARY] if self.enabled else []
            await send_json_error(
                send,
                415,
                "Unsupported Content-Encoding",
                [(b"accept-encoding", b"zstd, gzip"), *vary],
            )
            return
        await self.app(_with_accept_encoding(scope, choose(accept)), receive, send)
