import logging
from contextlib import AsyncExitStack, asynccontextmanager
from urllib.parse import urlsplit

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

from core.auth import BearerAuthMiddleware
from core.boundary_timing import ServerTimingMiddleware
from core.canonical_path import CanonicalPathMiddleware
from core.correlation import CorrelationIdMiddleware
from core.errors import JsonErrorBodyMiddleware, register_exception_handlers
from core.last_request import LastRequest, LastRequestMiddleware
from core.logging_config import configure_logging
from core.request_limits import MaxBodySizeMiddleware
from core.tracing import configure_tracing
from ingestion.noop.client import NoopBatchConsumer
from ingestion.solace.client import SolaceBatchConsumer
from mcp_routers import tools
from persistence.analytics_store.clickhouse.clickhouse_client import ClickHouseClient
from persistence.cache_store.redis.redis_client import RedisClient
from persistence.stream_store.lsm_store import LSMStore
from persistence.transaction_store.postgres.postgres_client import PostgresClient
from routers import cache, config, data, health, metrics
from schemas.root import RootInfo
from services.app_services import AppServices
from services.cache import CacheService
from services.config import ConfigService
from services.data import DataService
from services.health import HealthService
from services.metrics import MetricsService
from services.stream_ingest import StreamIngestService
from settings import Settings, get_settings

log = logging.getLogger(__name__)

# A client dropping its connection mid-response is routine, not an asyncio error.
logging.getLogger("asyncio").addFilter(
    lambda r: not (r.exc_info and isinstance(r.exc_info[1], ConnectionResetError))
)

_CONSUMERS = {
    "solace": SolaceBatchConsumer,
    "noop": NoopBatchConsumer,
}


def create_lifespan(
    settings: Settings,
    mcp: FastMCP,
    last_request: LastRequest,
    metrics_service: MetricsService | None,
):
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        async with AsyncExitStack() as stack:
            if app.state.tracer_provider is not None:
                # Runs last on exit: flushes spans buffered during shutdown too.
                stack.callback(app.state.tracer_provider.shutdown)
            # Each client connects with backoff, smoke-tests the store and (for
            # Postgres and ClickHouse) applies pending migrations before returning,
            # so everything below runs against live, migrated stores.
            pg_pool = await stack.enter_async_context(PostgresClient(settings))
            redis_client = await stack.enter_async_context(RedisClient(settings))
            ch_client = await stack.enter_async_context(ClickHouseClient(settings))

            consumer = await stack.enter_async_context(
                _CONSUMERS[settings.ingest_transport](settings)
            )
            store = LSMStore(
                flush_rows=settings.lsm_flush_rows,
                compaction_runs=settings.lsm_compaction_runs,
                key_columns=settings.lsm_key_columns,
            )
            ingest = await stack.enter_async_context(
                StreamIngestService(consumer, store, settings)
            )

            config_service = ConfigService(
                pg_pool, settings.postgres_acquire_timeout_seconds
            )
            cache_service = CacheService(redis_client)
            data_service = DataService(ch_client)
            app.state.services = AppServices(
                health=HealthService(
                    settings,
                    pings={
                        "postgres": config_service.ping,
                        "clickhouse": data_service.ping,
                        "redis": cache_service.ping,
                    },
                    ingest=ingest,
                    last_request=last_request,
                ),
                config=config_service,
                cache=cache_service,
                data=data_service,
                stream_ingest=ingest,
                metrics=metrics_service,
            )
            if metrics_service is not None:
                metrics_service.bind_ingest(ingest)

            await stack.enter_async_context(mcp.session_manager.run())
            yield

    return lifespan


def mcp_transport_security(settings: Settings) -> TransportSecuritySettings:
    """DNS-rebinding protection for /mcp built from `mcp_allowed_hosts`.

    FastMCP's own default only trusts `localhost:<port>`/`127.0.0.1:<port>` over
    http, which rejects `Host: localhost` on 443 and every deployed host name.
    """
    hosts: list[str] = []
    for host in settings.mcp_allowed_hosts:
        hosts.append(host)
        if urlsplit(f"//{host}").port is None:  # no explicit port: allow any
            hosts.append(f"{host}:*")
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=hosts,
        allowed_origins=[
            f"{scheme}://{h}" for h in hosts for scheme in ("https", "http")
        ],
    )


def create_app(settings: Settings) -> FastAPI:
    """Build a fully isolated application instance.

    Everything stateful (services, the FastMCP server whose session manager can
    only run once per instance, the lifespan) is created fresh per call, so
    multiple apps can coexist in one process (e.g. test apps with different
    transports in the same pytest session).
    """
    configure_logging(settings.log_level)
    last_request = LastRequest()
    metrics_service = MetricsService(settings) if settings.metrics_enabled else None

    mcp = FastMCP(
        name=settings.mcp_name,
        streamable_http_path="/",
        instructions=settings.mcp_instructions,
        transport_security=mcp_transport_security(settings),
    )

    app = FastAPI(
        title=settings.app_title,
        version=settings.app_version,
        description=settings.app_description,
        openapi_tags=[
            health.TAG_METADATA,
            data.TAG_METADATA,
            config.TAG_METADATA,
            cache.TAG_METADATA,
        ],
        lifespan=create_lifespan(settings, mcp, last_request, metrics_service),
    )
    tools.register(mcp, lambda: app.state.services)
    register_exception_handlers(app)

    # Every middleware here is pure ASGI (no BaseHTTPMiddleware), so the 413 that
    # MaxBodySizeMiddleware raises mid-read reaches the handler as a plain
    # HTTPException rather than an ExceptionGroup. Starlette applies them LIFO:
    # the last one added is the outermost. Outside-in order: CanonicalPath (so every
    # layer, auth included, sees the routed path), MaxBodySize, CorrelationId, CORS,
    # BearerAuth (if enabled), ServerTiming, LastRequest, then routing and the /mcp
    # mount (whose Host allow-list is FastMCP's own transport security).
    app.add_middleware(LastRequestMiddleware, last_request=last_request)
    # Just outside LastRequestMiddleware, so `total` covers handler + downstream I/O.
    app.add_middleware(ServerTimingMiddleware)
    if settings.auth_enabled:
        # Inside CORS (preflights need no token), outside routing (covers /mcp).
        app.add_middleware(BearerAuthMiddleware, settings=settings)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_allow_origins,
        allow_methods=settings.cors_allow_methods,
        allow_headers=settings.cors_allow_headers,
        allow_credentials=settings.cors_allow_credentials,
    )
    app.add_middleware(
        CorrelationIdMiddleware, header_name=settings.correlation_id_header
    )
    app.add_middleware(MaxBodySizeMiddleware, max_bytes=settings.max_request_body_bytes)
    # Outermost: `/x/` is routed as `/x`, and `/mcp` as the mount root `/mcp/`.
    app.add_middleware(CanonicalPathMiddleware)

    app.include_router(health.router, prefix="/health")
    app.include_router(data.router, prefix="/data")
    app.include_router(config.router, prefix="/config")
    app.include_router(cache.router, prefix="/cache")

    if metrics_service is not None:
        metrics_service.instrument(app)
        app.include_router(metrics.router)

    app.mount("/mcp", JsonErrorBodyMiddleware(mcp.streamable_http_app()))
    app.state.tracer_provider = configure_tracing(app, settings)

    @app.get("/", tags=["API Root Page"])
    async def get_root() -> RootInfo:
        return RootInfo(
            title=settings.app_title,
            version=settings.app_version,
            description=settings.app_description,
            docs="/docs",
            MCP="/mcp",
        )

    return app


# Module-level app for `uvicorn main:app`.
# All Settings fields have defaults so this is safe to execute at import time.
# A field without a default would need `uvicorn main:create_app --factory`.
app = create_app(get_settings())


if __name__ == "__main__":
    import uvicorn

    settings = get_settings()  # same cached instance as `app` above
    uvicorn.run(
        app,
        host=settings.server_host,
        port=settings.server_port,
        ssl_keyfile=settings.ssl_keyfile,
        ssl_certfile=settings.ssl_certfile,
        timeout_graceful_shutdown=int(settings.shutdown_timeout_seconds),
        # Keep uvicorn's loggers on the app's JSON handler (core/logging_config.py).
        log_config=None,
    )
