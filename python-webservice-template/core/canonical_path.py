"""One path per route, so no handler needs a trailing-slash twin."""

from starlette.types import ASGIApp, Receive, Scope, Send


class CanonicalPathMiddleware:
    """`/config/` is routed as `/config`. A mount root such as `/mcp` is routed as
    `/mcp/`, the path the mounted app serves, instead of redirecting."""

    def __init__(self, app: ASGIApp, mount_roots: tuple[str, ...] = ("/mcp",)) -> None:
        self.app = app
        self._mount_roots = mount_roots

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            path = self._canonical(scope["path"])
            if path != scope["path"]:
                scope = {**scope, "path": path, "raw_path": path.encode()}
        await self.app(scope, receive, send)

    def _canonical(self, path: str) -> str:
        if path in self._mount_roots:
            return path + "/"
        if any(path.startswith(root + "/") for root in self._mount_roots):
            return path
        return path.rstrip("/") or "/"
