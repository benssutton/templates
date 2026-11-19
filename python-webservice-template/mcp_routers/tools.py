"""MCP tools mirroring the REST read endpoints: each calls the same service
method as its route and returns the same response model."""

from collections.abc import Callable

from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError
from pydantic import Field

from schemas.cache import CacheEntry
from schemas.config import ConfigEntry
from schemas.data import DataRowsResponse
from schemas.health import DetailedStatusResponse
from services.app_services import AppServices

Limit = Field(default=10, ge=1, le=100, description="Maximum rows to return")


def register(mcp: FastMCP, services: Callable[[], AppServices]) -> None:
    # MCP tools run outside FastAPI's request DI; `services` resolves the owning
    # app's services at call time (they exist only once the lifespan has started).

    @mcp.tool()
    async def get_health_status() -> DetailedStatusResponse:
        """Detailed application status, as GET /health/status."""
        return await services().health.detailed_status()

    @mcp.tool()
    async def list_config() -> list[ConfigEntry]:
        """All configuration entries ordered by key, as GET /config."""
        return await services().config.get_all()

    @mcp.tool()
    async def get_cache_entry(key: str) -> CacheEntry:
        """One cache entry, as GET /cache/{key}; an error when the key is absent."""
        entry = await services().cache.get(key)
        if entry is None:  # REST answers 404; MCP reports a tool error
            raise ToolError(f"Key '{key}' not found")
        return entry

    @mcp.tool()
    async def get_data(limit: int = Limit) -> DataRowsResponse:
        """Rows from the analytics store ordered by id, as GET /data."""
        return await services().data.get_data(limit=limit)

    @mcp.tool()
    async def get_cached_data(limit: int = Limit) -> DataRowsResponse:
        """Rows from the in-memory stream store, as GET /data/cache."""
        return await services().stream_ingest.get_data(limit=limit)
