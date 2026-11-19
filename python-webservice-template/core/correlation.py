"""Request/ingest correlation identity, propagated via a ContextVar.

A single ID is carried for the lifetime of an HTTP request (set by
CorrelationIdMiddleware) or a single ingested batch (set by the ingest loop).
The logging filter stamps every record with the current value so one grep of
the ID surfaces the full causal trail. asyncio.to_thread copies the context, so
the HTTP /data/ingest path carries the request ID into the store-write thread
automatically; the streaming ingest thread sets its own per-batch ID.
"""

import contextvars
import logging
import re
import time
import uuid
from contextlib import asynccontextmanager

from core.boundary_timing import record_boundary
from starlette.datastructures import Headers, MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

correlation_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "correlation_id", default="-"
)


def get_correlation_id() -> str:
    return correlation_id_var.get()


def set_correlation_id(value: str) -> contextvars.Token:
    """Set the correlation ID and return the reset token.

    Callers that need to restore the previous value should pair this with
    `correlation_id_var.reset(token)` when done — e.g. the streaming ingest
    loop resets after each batch so backoff/error logs are not mis-attributed
    to the last successful batch's ID. Fire-and-forget callers that never need
    to restore the prior value may discard the returned token.
    """
    return correlation_id_var.set(value)


def new_id() -> str:
    return uuid.uuid4().hex


# An inbound id ends up in every log line and response header, so only a short,
# log-safe token is adopted (no spaces, newlines or delimiters to forge records).
_SAFE_ID = re.compile(r"[A-Za-z0-9._-]{1,128}")


def adopt_or_new_id(inbound: str | None) -> str:
    if inbound is not None and _SAFE_ID.fullmatch(inbound):
        return inbound
    return new_id()


class CorrelationIdFilter(logging.Filter):
    """Injects the current correlation ID onto every LogRecord."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = correlation_id_var.get()
        return True


class CorrelationIdMiddleware:
    """Adopts the inbound correlation header (or mints an ID), exposes it to the
    endpoint and its loggers via the ContextVar, and echoes it on the response.

    Pure ASGI, not BaseHTTPMiddleware: BaseHTTPMiddleware runs receive() inside a
    task group, which wraps errors raised mid-read (e.g. the 413 from
    MaxBodySizeMiddleware) in an ExceptionGroup, and it buffers streaming responses.
    """

    def __init__(self, app: ASGIApp, header_name: str = "X-Request-ID") -> None:
        self.app = app
        self._header = header_name

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        cid = adopt_or_new_id(Headers(scope=scope).get(self._header))

        async def send_with_id(message: Message) -> None:
            if message["type"] == "http.response.start":
                MutableHeaders(scope=message)[self._header] = cid
            await send(message)

        token = correlation_id_var.set(cid)
        try:
            await self.app(scope, receive, send_with_id)
        finally:
            correlation_id_var.reset(token)


_timing_log = logging.getLogger(__name__)


@asynccontextmanager
async def timed(label: str):
    """Log the wall-clock duration of an awaited boundary, tagged with the
    current correlation ID (added by CorrelationIdFilter), and record it as a
    Server-Timing sample for the current request (added by record_boundary).
    Structured as a context manager so an OpenTelemetry span could wrap the same
    boundary later without changing call sites."""
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed_ms = (time.perf_counter() - start) * 1000
        _timing_log.debug("%s %.2fms", label, elapsed_ms)
        record_boundary(label, elapsed_ms)
