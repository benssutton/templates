"""One path per route, so no handler needs a trailing-slash twin."""

from starlette.types import ASGIApp, Receive, Scope, Send


class CanonicalPathMiddleware:
    """`/config/` is routed as `/config`; a mount root `/mcp` as `/mcp/`, no redirect.

    Only a literal trailing `/` is stripped: the check uses the still-encoded
    `raw_path`, so `/x/a%2F` is left alone; the new `raw_path` keeps its encoding."""

    def __init__(self, app: ASGIApp, mount_roots: tuple[str, ...] = ("/mcp",)) -> None:
        self.app = app
        self._mount_roots = mount_roots

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            scope = self._canonical(scope)
        await self.app(scope, receive, send)

    def _canonical(self, scope: Scope) -> Scope:
        path: str = scope["path"]
        raw: bytes | None = scope.get("raw_path")
        if path in self._mount_roots:
            raw_root = raw + b"/" if raw is not None else (path + "/").encode()
            return {**scope, "path": path + "/", "raw_path": raw_root}
        if any(path.startswith(root + "/") for root in self._mount_roots):
            return scope
        if raw is None:  # no encoded form to consult: strip the decoded path
            stripped = path.rstrip("/") or "/"
            if stripped == path:
                return scope
            return {**scope, "path": stripped, "raw_path": stripped.encode()}
        trimmed = len(raw) - len(raw.rstrip(b"/"))
        if trimmed == 0 or raw == b"/":
            return scope
        # Each literal trailing `/` in raw_path is also a trailing `/` in path.
        return {
            **scope,
            "path": path[:-trimmed] or "/",
            "raw_path": raw[:-trimmed] or b"/",
        }
