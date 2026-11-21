"""Correlation ID for an HTTP request or an ingested batch, held in a ContextVar.

The logging filter stamps it on every record. asyncio.to_thread copies the
context, so /data/ingest's store-write thread keeps the request ID; the streaming
ingest thread sets its own per-batch ID.
"""

import contextvars
import logging
import re
import uuid

from starlette.datastructures import Headers, MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

correlation_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "correlation_id", default="-"
)


def get_correlation_id() -> str:
    return correlation_id_var.get()


def set_correlation_id(value: str) -> contextvars.Token:
    """Set the correlation ID and return the token that restores the previous one.

    The ingest loop resets after each batch so its backoff logs don't get that ID."""
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
    """Adopts the inbound correlation header (or mints an ID) and echoes it back.

    Pure ASGI: BaseHTTPMiddleware wraps errors raised mid-read (the 413 from
    MaxBodySizeMiddleware) in an ExceptionGroup and buffers streaming responses."""

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
