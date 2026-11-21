"""Per-request boundary timings, returned as a W3C Server-Timing header.

ServerTimingMiddleware puts a fresh sample list on a ContextVar per HTTP request
(so the ingest thread never pollutes request samples); timed() appends
(label, ms) and the header adds a `total` for the whole handler.
Pure ASGI, so streaming responses (/mcp) aren't buffered and the ContextVar
reaches the endpoint.
"""

import contextvars
import logging
import re
import time
from contextlib import asynccontextmanager

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

log = logging.getLogger(__name__)

# None when no HTTP request is in flight (e.g. the streaming ingest thread):
# record_boundary is a no-op and timed() only logs.
boundary_samples_var: contextvars.ContextVar[list[tuple[str, float]] | None] = (
    contextvars.ContextVar("boundary_samples", default=None)
)

# Metric names must be tokens; hyphens become `_` too, so every name matches the
# k6 parser's [A-Za-z0-9_]+.
_TOKEN_RE = re.compile(r"[^A-Za-z0-9_]")


def record_boundary(label: str, milliseconds: float) -> None:
    """Append a sample to the current request's list; no-op outside a request."""
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

        samples: list[tuple[str, float]] = []
        token = boundary_samples_var.set(samples)
        start = time.perf_counter()

        async def send_wrapper(message: Message) -> None:
            if message["type"] == "http.response.start":
                total_ms = (time.perf_counter() - start) * 1000
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


_timing_log = logging.getLogger(__name__)


@asynccontextmanager
async def timed(label: str):
    """Log the duration of an awaited block at DEBUG and record it as a
    Server-Timing sample."""
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed_ms = (time.perf_counter() - start) * 1000
        _timing_log.debug("%s %.2fms", label, elapsed_ms)
        record_boundary(label, elapsed_ms)
