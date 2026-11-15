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
ingestion/                        BatchConsumer (solace/noop), Delivery, ArrowDecoder
mcp/                              MCP JSON-RPC endpoint at /mcp (get_health_status tool)
publisher/                        ArrowBatches + ExampleSolacePublisher (docker-compose demo data)
src/main/resources/db/            SQL DDL (postgres-init.sql, clickhouse-init.sql) — single source of truth
src/test/.../support/             IntegrationSupport (PG+CH+Redis) + SolaceSupport (one broker per JVM) + Semp
tests/performance/                k6 scripts (reused from the Python template)
```

## Stack
- Micronaut 5 + Netty, Java 25, Maven, virtual threads (`@ExecuteOn(TaskExecutors.BLOCKING)`)
- Micronaut Data JDBC (Postgres, pgvector image), clickhouse-jdbc, Jedis + RedisJSON
- Apache Arrow + Solace Guaranteed messaging ingestion
- Micronaut Serialization, Jakarta Validation, Micrometer/Prometheus, OpenAPI
- JUnit 5 + Micronaut Test + Testcontainers; k6; GitLab CI; JaCoCo gate

## Key patterns
- **DI is framework-native.** Python's hand-rolled `Container` → Micronaut `ApplicationContext`; `create_app` isolation → `@MicronautTest` per class.
- **Health probe registry.** Each store implements `DependencyHealthProbe`; `HealthService` injects `List<DependencyHealthProbe>` — new stores self-register. Ingest health flows through `IngestHealthProvider` (real `StreamIngestService` or a default).
- **Schema on startup.** `SchemaInitializer` / `ClickHouseSchemaInitializer` run the shared `db/*.sql` on `StartupEvent` (the lifespan analogue); the injected `DataSource` is unwrapped via `DelegatingDataSource.unwrapDataSource(...)`.
- **Correlation ID** rides Micronaut `PropagatedContext` (`MdcPropagationContext`) into MDC on the virtual-thread handler. **Server-Timing** boundaries live on a request attribute, appended by `Timed` and rendered by `ServerTimingFilter`.
- **Settle after write.** `SolaceBatchConsumer` binds a self-provisioned durable exclusive queue (`CREATE_ON_START`, topic subscription attached) with client ack and hands one `Delivery` per message to `StreamIngestService`, which decides and settles each exactly once: ACCEPT after the store write; FAIL on a store error (broker redelivers; a jittered, doubling settlement backoff of 1-60 s prevents a hot loop); REJECT for oversized (`max-batch-bytes`), malformed or unmergeable batches (never redelivered). Auto-created queues have unlimited redelivery and no DMQ; production queues should set both. Default transport is `solace`; `INGEST_TRANSPORT=noop` is the opt-out for HTTP-only ingest; under compose `app` depends on a healthy `solace`, so use `docker compose up -d postgres clickhouse redis && INGEST_TRANSPORT=noop docker compose up -d --no-deps app`.
- **Resilience.** `Retry.connectWithBackoff`; `StartupSmoke` fail-fast on `StartupEvent` (disabled under the test env). The ingest loop retries transport failures with jittered exponential backoff; SDK lifecycle listeners drive `ConnectionState`. A successful bind (`consume(sink, onBound)` runs `onBound` on the ingest thread once the persistent receiver starts) or a delivery resets the backoff and failure count. 5 consecutive failures without a successful bind, or the disconnect watchdog (`max-disconnect-seconds`), call the shutdown hook, which runs `System.exit(3)` on a separate non-blocking thread. Graceful shutdown relies on `@Bean(preDestroy = "close")` in `StreamIngestBeans`, because Micronaut 5 doesn't infer `close()` on factory beans.

## Deliberate divergences from the Python template
1. **Native DI** (no hand-rolled container).
2. **Append-only LSM store** — compaction pushed to the client, so `/data/cache` rows carry `seqno`+`op` (the one response-model difference).
3. **Jedis** for RedisJSON (vs Lettuce).
4. **async-profiler** for Layer-2 profiling (vs py-spy).
5. **MCP via a JSON-RPC `@Controller`** (the SDK's transports are servlet/SSE-oriented; a thin Netty controller speaks the same protocol for the single tool).
6. **Solace connect timing.** Java connects lazily inside the ingest loop (each `consume()` builds a fresh `MessagingService`), so the app comes up live with ingest DOWN when the broker is unreachable, and the failure path (5 consecutive failures without a successful bind) or the disconnect watchdog exits. Python connects with backoff in the lifespan and fails startup. Both reconnect with a fresh service after a terminal interruption.
7. **Batch size limit.** Java checks the encoded message payload against `max-batch-bytes` (all-or-nothing, before any write); Python checks the decoded Arrow `nbytes` of each batch, also before any write.

## Environment gotchas (see also docs/superpowers/specs + plans)
- The Micronaut 5.0.2 BOM pins `testcontainers` to a non-existent 2.0.5 → overridden to 1.21.3.
- Arrow vector needs `--add-opens=java.base/java.nio=ALL-UNNAMED` (in surefire argLine + Docker `JAVA_TOOL_OPTIONS`).
- On Windows + Docker Desktop, the `windows-docker` Maven profile pins `DOCKER_HOST` for Testcontainers; inactive on Linux/CI.
- Tests run the embedded server on a random port (`src/test/resources/application-test.yml`, `micronaut.server.port: -1`) because Solace SEMP uses 8080.
- Micronaut 5.0.2 races `getParallelBeans()` against a fast context close (NPE on an untracked thread, surfaced by Awaitility in whichever test is mid-await); `ConfigDtoSerdeTest` disables eager beans to dodge it — fixed upstream in platform 5.2.0+ (micronaut-core#13010).

## Database investigation
Run the relevant test — each spins fresh Testcontainers: `./mvnw test -Dtest=ConfigControllerTest` (Postgres), `-Dtest=DataControllerTest` (ClickHouse), `-Dtest=CacheControllerTest` (Redis). Never reuse a developer's running container.
