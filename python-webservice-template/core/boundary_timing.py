"""Request-scoped capture of per-boundary wall-clock timings, surfaced as a
W3C Server-Timing response header.

ServerTimingMiddleware installs a fresh sample list on a ContextVar at the start
of each HTTP request; core.correlation.timed() appends (label, milliseconds) to
that list as each instrumented boundary completes. On the response's
`http.response.start` the middleware renders the samples — plus a synthetic
`total` for the whole handler — into a Server-Timing header.

Request-scoping (the same isolation model as the correlation ID) means work that
runs outside a request — e.g. the streaming ingest thread — never pollutes a
request's samples, and multiple isolated apps in one test process cannot collide.

Implemented as pure ASGI (mirroring core.request_limits.MaxBodySizeMiddleware) so
it does not buffer streaming responses (the /mcp mount) and propagates the
ContextVar to the endpoint reliably.
"""
import contextvars
import logging
import re
import time

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

log = logging.getLogger(__name__)

# None when no HTTP request is in flight (e.g. the streaming ingest thread):
# record_boundary becomes a no-op and timed() only logs, exactly as before.
boundary_samples_var: contextvars.ContextVar[list[tuple[str, float]] | None] = (
    contextvars.ContextVar("boundary_samples", default=None)
)

# Server-Timing metric names must be tokens; map any other char to underscore.
_TOKEN_RE = re.compile(r"[^A-Za-z0-9_]")


def record_boundary(label: str, milliseconds: float) -> None:
    """Append a boundary sample to the current request's list, if one is active.

    No-op outside an HTTP request (the list is None), so non-request callers such
    as the streaming ingest thread are unaffected."""
    samples = boundary_samples_var.get()
    if samples is not None:
        samples.append((label, milliseconds))


def _render_header(samples: list[tuple[str, float]], total_ms: float) -> str:
    """Render samples into a Server-Timing value, summing duplicate labels (e.g.
    one ingest.lsm_write per batch) and preserving first-seen order."""
    aggregated: dict[str, float] = {}
    order: list[str] = []
    for label, ms in samples:
        token = _TOKEN_RE.sub("_", label)
        if token not in aggregated:
            order.append(token)
            aggregated[token] = 0.0
        aggregated[token] += ms
    parts = [f"{token};dur={aggregated[token]:.2f}" for token in order]
    parts.append(f"total;dur={total_ms:.2f}")
    return ", ".join(parts)


class ServerTimingMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        token = boundary_samples_var.set([])
        start = time.perf_counter()

        async def send_wrapper(message: Message) -> None:
            if message["type"] == "http.response.start":
                total_ms = (time.perf_counter() - start) * 1000
                samples = boundary_samples_var.get() or []
                try:
                    MutableHeaders(scope=message)["Server-Timing"] = _render_header(
                        samples, total_ms
                    )
                except Exception:  # diagnostics must never break the response
                    log.exception("failed to render Server-Timing header")
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            boundary_samples_var.reset(token)
