"""Records when the app last received an HTTP request (shown by /health/status).

Pure ASGI: BaseHTTPMiddleware would turn MaxBodySizeMiddleware's mid-read 413
into an ExceptionGroup.
"""

from datetime import UTC, datetime

from starlette.types import ASGIApp, Receive, Scope, Send


class LastRequest:
    """Per-app holder, so isolated apps in one process never share the value."""

    def __init__(self) -> None:
        self.at: datetime | None = None


class LastRequestMiddleware:
    def __init__(self, app: ASGIApp, last_request: LastRequest) -> None:
        self.app = app
        self._last_request = last_request

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            self._last_request.at = datetime.now(UTC)
        await self.app(scope, receive, send)
