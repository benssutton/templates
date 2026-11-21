# Java Template

Micronaut 5 (platform 5.2.5) on Java 25. Read [README.md](README.md) for endpoints,
config, ingest semantics and the "Adding things" recipes. The wire contract (paths,
snake_case JSON, `{"detail"}` errors, statuses, env vars) is shared with the Python and
Rust templates and pinned by raw-JSON tests (`ContractTest`); keep it unchanged unless
all three change.

## Commands (from this directory; JDK 25 + Docker running)
```bash
./mvnw test                               # fast: *Test classes, no Solace broker (*IT need it)
./mvnw verify                             # everything + JaCoCo gate (Solace broker ~1 min to start)
./mvnw test -Dtest=CacheControllerTest    # one class
./mvnw spotless:apply                     # format; spotless:check is the gate
./mvnw -Plint -DskipTests test-compile    # javac -Xlint -Werror
```
Repo root: `python run_CICD.py java fast|check|ci` (see root README "Pipeline").

## Layout
```
config/        AppSettings, IngestSettings: validated immutable @ConfigurationProperties records
auth/          bearer-JWT seam (off by default): settings, startup check, iss/aud validator, 401 handler
controller/    REST controllers, ErrorDetailResponseProcessor (the one error body), status-mapping handlers
dto/           @Serdeable records; health statuses are enums serialised lowercase
core/          correlation id (CorrelationId rule, MDC propagation), Server-Timing, retry, startup smoke, system metrics
health/        DependencyHealthProbe + PingProbe + DependencyProbes (one probe per store)
service/       Config/Cache/Data/CachedData/Health services, StreamIngestService (ingest loop + settlement)
ingestion/     BatchConsumer (noop default, solace), Delivery (raw payload), ArrowDecoder (shared bounded allocator)
persistence/   Postgres repo (Flyway db/postgres), ClickHouse repo + ClickHouseMigrations (db/clickhouse), Redis factory, LsmStore
observability/ MetricsBinder (gauges/counters, registered once), /metrics
mcp/           @Tool methods mirroring the REST reads, Host/Origin allow-list filter
src/test/.../support/    IntegrationSupport (PG+CH+Redis), SolaceSupport + Semp, TestSettings, ArrowBatches
src/test/.../publisher/  ExampleSolacePublisher (compose demo; Dockerfile target `publisher`)
```

## Gotchas
- Always `./mvnw clean ...` after editing sources: incremental compile breaks on Windows
  when the drive-letter case of the source root flips (`/C:/` vs `/c:/`).
- The VS Code Java extension can lock `target/` during `clean`; rerun if Maven reports
  "Failed to delete" or "error reading" a generated file.
- `.mvn/jvm.config` exports javac internals for google-java-format (Spotless).
- Testcontainers is pinned to 1.21.4: the BOM's 2.0.5 renames the modules this project
  uses. Docker Engine 29 needs `api.version=1.43` (set in surefire and failsafe).
- Arrow needs `--add-opens=java.base/java.nio=ALL-UNNAMED` (test argLine, Docker `JAVA_TOOL_OPTIONS`).
- Factory beans need `@Bean(preDestroy = "close")`; Micronaut does not infer `close()` there.
- clickhouse-jdbc 0.11 (client-v2): `data-source-properties` values must be quoted strings, server
  settings take the `clickhouse_setting_` prefix, and `jdbc_ignore_unsupported_values` keeps
  Micronaut's `setAutoCommit(false)` from throwing (ClickHouse has no transactions).
- A thrown exception from an MCP `@Tool` becomes a JSON-RPC error; return `CallToolResult.isError` instead.
- `HttpStatus` implements `CharSequence`: `assertThat(status)` is ambiguous; cast to `Object`.
- Tests never reuse a developer's running containers; each JVM starts its own.
- Surefire runs test classes four at a time (`test.parallelism` in `pom.xml`); methods within a class
  stay on one thread. `@Isolated` marks classes that touch JVM-global state (none today). Failsafe
  `*IT` classes run sequentially (one shared Solace broker). Integration tests must store only
  `unique(prefix)` keys. `-Dtest.parallelism=1` runs serially for debugging.
