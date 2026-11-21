"""Caps the inbound request body size, app-wide.

Counts bytes as they stream and aborts with 413 once the limit is crossed, so an
oversized body is never buffered or handled, even without Content-Length.
"""

import json

from starlette.exceptions import HTTPException
from starlette.types import ASGIApp, Message, Receive, Scope, Send

_DETAIL = "Payload Too Large"
_BODY = json.dumps({"detail": _DETAIL}).encode()


class _BodyTooLarge(HTTPException):
    """Raised from receive() to unwind the handler mid-read.

    An HTTPException because FastAPI's body parsing re-raises those but turns any
    other error into 400."""

    def __init__(self) -> None:
        super().__init__(status_code=413, detail=_DETAIL)


class MaxBodySizeMiddleware:
    def __init__(self, app: ASGIApp, max_bytes: int) -> None:
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # Fast path: trust an explicit, oversized Content-Length and reject early.
        for name, value in scope.get("headers", []):
            if name == b"content-length":
                try:
                    if int(value) > self.max_bytes:
                        await self._reject(send)
                        return
                except ValueError:
                    pass
                break

        received = 0
        too_large = False
        response_started = False
        rejected = False

        async def counting_receive() -> Message:
            nonlocal received, too_large
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > self.max_bytes:
                    too_large = True
                    raise _BodyTooLarge
            return message

        async def tracking_send(message: Message) -> None:
            nonlocal response_started, rejected
            # A handler may swallow the exception and answer itself (the MCP SDK
            # answers 500): replace that response with the 413 while we still can.
            if too_large and not response_started:
                if not rejected:
                    rejected = True
                    await self._reject(send)
                return
            if rejected:
                return
            if message["type"] == "http.response.start":
                response_started = True
            await send(message)

        try:
            await self.app(scope, counting_receive, tracking_send)
        except _BodyTooLarge:
            # A handler that streams a response before reading its whole body has
            # already sent headers; the connection can only be dropped then.
            if response_started:
                raise
            if not rejected:
                await self._reject(send)

    @staticmethod
    async def _reject(send: Send) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": 413,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(_BODY)).encode()),
                ],
            }
        )
        await send({"type": "http.response.body", "body": _BODY})
