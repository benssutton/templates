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
import time
import uuid
from contextlib import asynccontextmanager

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

correlation_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "correlation_id", default="-"
)


def get_correlation_id() -> str:
    return correlation_id_var.get()


def set_correlation_id(value: str) -> contextvars.Token:
    """Set the correlation ID and return the reset token.

    Callers that need to restore the previous value (e.g. per-batch ingest
    loops that want clean isolation between batches) should call
    `correlation_id_var.reset(token)` when done.  Fire-and-forget callers
    (e.g. the streaming ingest thread) can safely discard the return value.
    """
    return correlation_id_var.set(value)


def new_id() -> str:
    return uuid.uuid4().hex


class CorrelationIdFilter(logging.Filter):
    """Injects the current correlation ID onto every LogRecord."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = correlation_id_var.get()
        return True


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, header_name: str = "X-Request-ID") -> None:
        super().__init__(app)
        self._header = header_name

    async def dispatch(self, request: Request, call_next):
        # Set BEFORE call_next so the value is visible to the downstream endpoint
        # and its loggers (the reliable direction for BaseHTTPMiddleware + contextvars).
        incoming = request.headers.get(self._header)
        cid = incoming or new_id()
        token = correlation_id_var.set(cid)
        try:
            response = await call_next(request)
        finally:
            correlation_id_var.reset(token)
        response.headers[self._header] = cid
        return response


_timing_log = logging.getLogger(__name__)


@asynccontextmanager
async def timed(label: str):
    """Log the wall-clock duration of an awaited boundary, tagged with the
    current correlation ID (added by CorrelationIdFilter). Structured as a
    context manager so an OpenTelemetry span could wrap the same boundary later
    without changing call sites."""
    start = time.perf_counter()
    try:
        yield
    finally:
        _timing_log.debug("%s %.2fms", label, (time.perf_counter() - start) * 1000)
