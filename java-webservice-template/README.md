# java-webservice-template

A Micronaut (Java 25) service that mirrors `python-webservice-template`
feature-for-feature, built so a developer can read either codebase and map one
onto the other. Same endpoints, same request/response shapes, minimal business
logic — a reusable foundation wired to common technologies.

Design + Python↔Java mapping: `../docs/superpowers/specs/2026-06-15-java-webservice-template-design.md`.

## Stack
- **Micronaut 5** on **Java 25** (Temurin), **Maven**
- **Virtual threads** — controllers run blocking handlers via `@ExecuteOn(TaskExecutors.BLOCKING)`
- **Micronaut Data JDBC** + HikariCP (Postgres), **clickhouse-jdbc** (ClickHouse), **Jedis** + RedisJSON
- **Apache Arrow** + **Arrow Flight** / **Solace** stream ingestion
- **Micronaut Serialization**, Jakarta Validation, **Micrometer/Prometheus**, OpenAPI/Swagger-UI
- **JUnit 5 + Micronaut Test + Testcontainers** (real dependencies, no mocks)

## Feature map (entry points)

| Feature | Entry point |
|---|---|
| App bootstrap (the `create_app`/DI analogue) | [Application.java](src/main/java/com/example/template/Application.java) |
| Typed settings | [config/AppSettings.java](src/main/java/com/example/template/config/AppSettings.java) + [application.yml](src/main/resources/application.yml) |
| `/config` (Postgres, Micronaut Data) | [controller/ConfigController.java](src/main/java/com/example/template/controller/ConfigController.java) |
| `/data` (ClickHouse) | [controller/DataController.java](src/main/java/com/example/template/controller/DataController.java) |
| `/data/cache` + `/data/ingest` (LSM) | [controller/DataController.java](src/main/java/com/example/template/controller/DataController.java) |
| `/cache` (Redis + RedisJSON) | [controller/CacheController.java](src/main/java/com/example/template/controller/CacheController.java) |
| `/health/{live,ready,status}` | [controller/HealthController.java](src/main/java/com/example/template/controller/HealthController.java), [service/HealthService.java](src/main/java/com/example/template/service/HealthService.java) |
| `/metrics` (Prometheus) | [observability/MetricsController.java](src/main/java/com/example/template/observability/MetricsController.java) |
| Correlation-ID (MDC propagation) | [core/CorrelationFilter.java](src/main/java/com/example/template/core/CorrelationFilter.java) |
| Server-Timing attribution | [core/ServerTimingFilter.java](src/main/java/com/example/template/core/ServerTimingFilter.java), [core/Timed.java](src/main/java/com/example/template/core/Timed.java) |
| Simplified append-only LSM store | [persistence/streamstore/LsmStore.java](src/main/java/com/example/template/persistence/streamstore/LsmStore.java) |
| Stream ingestion (thread, backoff, watchdog) | [service/StreamIngestService.java](src/main/java/com/example/template/service/StreamIngestService.java) |
| Flight / Solace transports | [ingestion/flight/FlightBatchConsumer.java](src/main/java/com/example/template/ingestion/flight/FlightBatchConsumer.java), [ingestion/solace/SolaceBatchConsumer.java](src/main/java/com/example/template/ingestion/solace/SolaceBatchConsumer.java) |
| Fail-fast startup smoke-test | [core/StartupSmoke.java](src/main/java/com/example/template/core/StartupSmoke.java) |
| Connect-with-backoff | [core/Retry.java](src/main/java/com/example/template/core/Retry.java) |
| Shared Testcontainers base | [test/.../support/IntegrationSupport.java](src/test/java/com/example/template/support/IntegrationSupport.java) |
| k6 performance tests | [tests/performance/](tests/performance/) |

## Build & test
```bash
cd java-webservice-template
# JAVA_HOME must point at a JDK 25; Docker must be running (Testcontainers).
./mvnw verify      # runs all tests + JaCoCo coverage gate
```
On Windows + Docker Desktop, the `windows-docker` Maven profile auto-activates to
pin `DOCKER_HOST`; on Linux/CI it stays inactive.

## Run the full stack (HTTPS on 443)
```bash
docker compose up -d --build --wait
curl -fsk https://localhost/health/ready
# Swagger UI: https://localhost/swagger-ui  •  MCP: https://localhost/mcp
```

## How this maps to python-webservice-template
Micronaut's `ApplicationContext` replaces the Python hand-rolled DI container;
`@MicronautTest` replaces `create_app` isolation. Deliberate divergences:
native DI; **append-only LSM store** (compaction pushed to the client, so
`/data/cache` rows carry `seqno`+`op`); **Jedis** for RedisJSON; **async-profiler**
for Layer-2 profiling. Everything else mirrors the Python structure and naming.

## Performance profiling
See [PERF_PROFILING_RUNBOOK.md](PERF_PROFILING_RUNBOOK.md) — Layer 1 (k6 +
Server-Timing) and Layer 2 (async-profiler).
