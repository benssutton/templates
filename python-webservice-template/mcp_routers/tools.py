"""MCP tools mirroring the REST read endpoints: each calls the same service
method as its route and returns the REST body as one text content item."""

import json
from collections.abc import Callable

from mcp.server.mcpserver import MCPServer
from mcp.types import CallToolResult, TextContent
from pydantic import Field, TypeAdapter

from schemas.config import ConfigEntry
from services.app_services import AppServices

Limit = Field(default=10, ge=1, le=100, description="Maximum rows to return")

_CONFIG_LIST = TypeAdapter(list[ConfigEntry])


def _text(payload: str, *, is_error: bool = False) -> CallToolResult:
    """One text item holding the REST body, as every template's tools return."""
    return CallToolResult(
        content=[TextContent(type="text", text=payload)], is_error=is_error
    )


def register(mcp: MCPServer, services: Callable[[], AppServices]) -> None:
    # MCP tools run outside FastAPI's request DI; `services` resolves the owning
    # app's services at call time (they exist only once the lifespan has started).

    @mcp.tool()
    async def get_health_status() -> CallToolResult:
        """Detailed application status, as GET /health/status."""
        status = await services().health.detailed_status()
        return _text(status.model_dump_json())

    @mcp.tool()
    async def get_config() -> CallToolResult:
        """All configuration entries ordered by key, as GET /config."""
        entries = await services().config.get_all()
        return _text(_CONFIG_LIST.dump_json(entries).decode())

    @mcp.tool()
    async def get_cache(key: str) -> CallToolResult:
        """One cache entry, as GET /cache/{key}; isError with the 404 body when absent."""
        entry = await services().cache.get(key)
        if entry is None:
            # The REST 404 body for GET /cache/{key}.
            return _text(
                json.dumps({"detail": f"Key '{key}' not found"}), is_error=True
            )
        return _text(entry.model_dump_json())

    @mcp.tool()
    async def get_data(limit: int = Limit) -> CallToolResult:
        """Rows from the analytics store ordered by id, as GET /data."""
        rows = await services().data.get_data(limit=limit)
        return _text(rows.model_dump_json())

    @mcp.tool()
    async def get_cached_data(limit: int = Limit) -> CallToolResult:
        """Rows from the in-memory stream store, as GET /data/cache."""
        rows = await services().stream_ingest.get_data(limit=limit)
        return _text(rows.model_dump_json())
