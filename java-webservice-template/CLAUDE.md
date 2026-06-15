# Java Template

A Micronaut (Java 25) webservice that mirrors `python-webservice-template`
feature-for-feature so the two codebases map onto each other. Same endpoints,
same request/response shapes, minimal business logic.

## Architecture
```
Application.java                  Micronaut entry point (the create_app/DI analogue)
config/AppSettings.java           Typed config (@ConfigurationProperties); env overrides
config/IngestSettings.java        Ingest/transport config
controller/                       REST endpoints (config, data, cache, health, root)
dto/                              @Serdeable records (request/response models)
core/                             Cross-cutting filters + utils (correlation, server-timing, retry, smoke-test, system-metrics)
health/                           DependencyHealthProbe registry + ingest-health seam
observability/                    Micrometer custom gauges + /metrics controller
persistence/
  transactionstore/postgres/      Micronaut Data JDBC repo (config)
  analyticsstore/clickhouse/      clickhouse-jdbc repo with @Query (data)
  cachestore/redis/               Jedis factory (cache, RedisJSON)
  streamstore/                    Simplified append-only LsmStore
ingestion/                        BatchConsumer (flight/solace/noop), ArrowDecoder
mcp/                              MCP JSON-RPC endpoint at /mcp (get_health_status tool)
flightserver/                     Standalone Arrow Flight server for docker-compose
src/main/resources/db/            SQL DDL (postgres-init.sql, clickhouse-init.sql) — single source of truth
src/test/.../support/             IntegrationSupport: shared singleton Testcontainers (PG+CH+Redis)
tests/performance/                k6 scripts (reused from the Python template)
```

## Stack
- Micronaut 5 + Netty, Java 25, Maven, virtual threads (`@ExecuteOn(TaskExecutors.BLOCKING)`)
- Micronaut Data JDBC (Postgres), clickhouse-jdbc, Jedis + RedisJSON
- Apache Arrow + Arrow Flight / Solace ingestion
- Micronaut Serialization, Jakarta Validation, Micrometer/Prometheus, OpenAPI
- JUnit 5 + Micronaut Test + Testcontainers; k6; GitLab CI; JaCoCo gate

## Key patterns
- **DI is framework-native.** Python's hand-rolled `Container` → Micronaut `ApplicationContext`; `create_app` isolation → `@MicronautTest` per class.
- **Health probe registry.** Each store implements `DependencyHealthProbe`; `HealthService` injects `List<DependencyHealthProbe>` — new stores self-register. Ingest health flows through `IngestHealthProvider` (real `StreamIngestService` or a default).
- **Schema on startup.** `SchemaInitializer` / `ClickHouseSchemaInitializer` run the shared `db/*.sql` on `StartupEvent` (the lifespan analogue); the injected `DataSource` is unwrapped via `DelegatingDataSource.unwrapDataSource(...)`.
- **Correlation ID** rides Micronaut `PropagatedContext` (`MdcPropagationContext`) into MDC on the virtual-thread handler. **Server-Timing** boundaries live on a request attribute, appended by `Timed` and rendered by `ServerTimingFilter`.
- **Resilience.** `Retry.connectWithBackoff`; `StartupSmoke` fail-fast on `StartupEvent` (disabled under the test env); ingest thread with backoff + consecutive-failure/disconnect → `System.exit(3)`.

## Deliberate divergences from the Python template
1. **Native DI** (no hand-rolled container).
2. **Append-only LSM store** — compaction pushed to the client, so `/data/cache` rows carry `seqno`+`op` (the one response-model difference).
3. **Jedis** for RedisJSON (vs Lettuce).
4. **async-profiler** for Layer-2 profiling (vs py-spy).
5. **MCP via a JSON-RPC `@Controller`** (the SDK's transports are servlet/SSE-oriented; a thin Netty controller speaks the same protocol for the single tool).

## Environment gotchas (see also docs/superpowers/specs + plans)
- The Micronaut 5.0.2 BOM pins `testcontainers` to a non-existent 2.0.5 → overridden to 1.21.3; and gRPC to 1.80.0 which breaks Arrow Flight 18.1.0 → pinned to 1.69.0.
- Arrow needs `--add-opens=java.base/java.nio=ALL-UNNAMED` (in surefire argLine + Docker `JAVA_TOOL_OPTIONS`).
- On Windows + Docker Desktop, the `windows-docker` Maven profile pins `DOCKER_HOST` for Testcontainers; inactive on Linux/CI.

## Database investigation
Run the relevant test — each spins fresh Testcontainers: `./mvnw test -Dtest=ConfigControllerTest` (Postgres), `-Dtest=DataControllerTest` (ClickHouse), `-Dtest=CacheControllerTest` (Redis). Never reuse a developer's running container.
