"""Bearer-token (JWT, HS256) authentication seam, off by default.

Pure ASGI rather than a FastAPI dependency so it also covers the mounted /mcp
app, which FastAPI's dependency system never sees. Sits inside CORSMiddleware so
browser preflights are answered without a token.
"""

import json

import jwt
from starlette.datastructures import Headers
from starlette.types import ASGIApp, Receive, Scope, Send

from settings import Settings

# Orchestrator probes carry no credentials.
EXEMPT_PATHS = frozenset({"/health/live", "/health/ready"})

_BODY = json.dumps({"detail": "Not authenticated"}).encode()


class BearerAuthMiddleware:
    def __init__(self, app: ASGIApp, settings: Settings) -> None:
        if settings.auth_jwt_secret is None:  # Settings rejects this when enabled
            raise ValueError("auth_jwt_secret is required when auth is enabled")
        self.app = app
        self._secret = settings.auth_jwt_secret.get_secret_value()
        self._issuer = settings.auth_jwt_issuer
        self._audience = settings.auth_jwt_audience

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope["path"] in EXEMPT_PATHS:
            await self.app(scope, receive, send)
            return
        if not self._authenticated(Headers(scope=scope).get("authorization", "")):
            await send(
                {
                    "type": "http.response.start",
                    "status": 401,
                    "headers": [
                        (b"content-type", b"application/json"),
                        (b"content-length", str(len(_BODY)).encode()),
                        (b"www-authenticate", b"Bearer"),
                    ],
                }
            )
            await send({"type": "http.response.body", "body": _BODY})
            return
        await self.app(scope, receive, send)

    def _authenticated(self, authorization: str) -> bool:
        scheme, _, token = authorization.partition(" ")
        if scheme.lower() != "bearer" or not token:
            return False
        try:
            jwt.decode(
                token,
                self._secret,
                algorithms=["HS256"],  # pinned: never trust the token's own alg
                issuer=self._issuer,
                audience=self._audience,
                # PyJWT rejects a token carrying `aud` when no audience is
                # configured; only check it when one is.
                options={"verify_aud": self._audience is not None},
            )
        except jwt.InvalidTokenError:
            return False
        return True
