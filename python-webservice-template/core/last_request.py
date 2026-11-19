"""Records when the app last received an HTTP request (shown by /health/status).

Pure ASGI for the same reason as CorrelationIdMiddleware: BaseHTTPMiddleware
(`@app.middleware("http")`) wraps receive() in a task group, which turns the
413 raised mid-read by MaxBodySizeMiddleware into an ExceptionGroup.
"""

from datetime import datetime, timezone

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
            self._last_request.at = datetime.now(timezone.utc)
        await self.app(scope, receive, send)
