# java-webservice-template

A Micronaut (Java 25) webservice template: REST endpoints over Postgres, ClickHouse,
Redis and an in-memory stream store fed by HTTP or Solace, with health, metrics, MCP
tools, and tests against real dependencies. It is one of three sibling templates
(Python, Java, Rust). Each is idiomatic in its own language; they share one wire
contract (paths, snake_case JSON, `{"detail"}` errors, status codes, env vars), and
parity is held by contract tests, not by mirroring code.

## Stack
- **Micronaut 5** (platform 5.2.5) on **Java 25**, Maven, Netty, virtual threads
  (`@ExecuteOn(TaskExecutors.BLOCKING)`)
- **Micronaut Data JDBC** + HikariCP + **Flyway** (Postgres, pgvector image),
  **clickhouse-jdbc** with app-applied versioned migrations, **Jedis** + RedisJSON
- **Apache Arrow** IPC ingest over HTTP or **Solace** Guaranteed messaging (opt-in)
- Micronaut Serialization (global snake_case), Jakarta Validation, Micrometer/Prometheus,
  OpenAPI/Swagger UI, micronaut-mcp (official MCP Java SDK), micronaut-security-jwt
  (off by default), OpenTelemetry tracing (export off by default), JSON logs
- JUnit 5 + Micronaut Test + Testcontainers (no mocks), JaCoCo, Spotless, javac lint

## Endpoints

| Path | What |
|---|---|
| `GET /` | title, version, description, `docs`, `MCP` |
| `GET /config`, `POST /config` | Postgres key/value configuration (POST 201) |
| `GET /cache/{key}`, `POST /cache` | RedisJSON values with optional `ttl_seconds` (POST 201, GET 404 when absent) |
| `GET /data?limit=1..100` | ClickHouse rows ordered by `id` |
| `GET /data/cache?limit=1..100` | newest LSM rows (oldest first), each with `seqno` and `op` |
| `POST /data/ingest` | Arrow IPC stream (`application/octet-stream`), 202 |
| `GET /health/live`, `/health/ready`, `/health/status` | liveness, readiness (503 when not ready), detailed status |
| `GET /metrics` | Prometheus text |
| `/mcp` | MCP tools mirroring the read endpoints |
| `/swagger-ui` | OpenAPI UI |

**Errors** are always `application/json {"detail": "..."}`: 400 unreadable body,
401 (auth on), 403 (`/mcp` Host/Origin rejected), 404, 405, 413 (body or batch too
large), 415, 422 (validation and conversion errors, invalid Arrow batch), 500 (no
internals), 503 (a store is down or timing out; detail `Service Unavailable`). One
`ErrorResponseProcessor` (`controller/ErrorDetailResponseProcessor`) renders every
error; exception handlers only pick the status.

**Correlation id.** `X-Request-ID` is adopted only if it is 1-128 chars of
`[A-Za-z0-9._-]`, otherwise a new UUID is generated; it is echoed on the response and
stamped on every log line (MDC `correlationId`), including health probe threads. A
Solace message's correlation id follows the same rule.

## Build, test, format, lint

JDK 25 and a running Docker (Testcontainers) are needed. From this directory:

```bash
./mvnw test                         # fast: *Test classes; no Solace broker (*IT classes need it)
./mvnw verify                       # all tests (Solace broker too) + JaCoCo gate
./mvnw test -Dtest=CacheControllerTest   # one class
./mvnw spotless:apply               # format (google-java-format, AOSP style)
./mvnw spotless:check               # format gate
./mvnw -Plint -DskipTests test-compile   # javac -Xlint -Werror
```

From the repo root the same steps run as `python run_CICD.py java fmt|fmt-check|lint|test-fast|test`
(`fast` = fmt-check + lint; `check` adds `test`; `ci` adds image build, scan and k6 smoke).

On Windows + Docker Desktop the `windows-docker` Maven profile auto-activates to pin
`DOCKER_HOST`. Tests run the embedded server on a random port because the Solace
broker's SEMP API uses 8080.

## Configuration

Settings are immutable, validated records (`config/AppSettings`, `config/IngestSettings`,
`auth/AuthSettings`); defaults live in `application.yml` and a value that breaks a
constraint fails startup.

| Env | Default | Meaning |
|---|---|---|
| `SERVER_PORT` | `8443` | HTTP listen port |
| `INGEST_TRANSPORT` | `noop` | `noop` (HTTP ingest only) or `solace` |
| `MCP_ALLOWED_HOSTS` | `localhost` | `/mcp` Host/Origin allow-list: JSON array or comma-separated `host[:port]` (no port = any port) |
| `AUTH_ENABLED` | `false` | require `Authorization: Bearer <HS256 JWT>` on every route but `/health/live` and `/health/ready` |
| `AUTH_JWT_SECRET` | (none) | HS256 key, at least 32 bytes; startup fails if auth is on without it |
| `AUTH_JWT_ISSUER`, `AUTH_JWT_AUDIENCE` | (none) | checked only when set |
| `LOG_FORMAT`, `LOG_LEVEL` | `json`, `INFO` | `json` or `text` logs; root level |
| `OTEL_TRACES_EXPORTER`, `OTEL_EXPORTER_OTLP_ENDPOINT` | `none` | set `otlp` to export traces (traceparent is always honoured) |
| `HEALTH_CACHE_TTL_SECONDS` | `5` | `/health/status` and `/metrics` reuse one probe round this long; readiness is always live |
| `POSTGRES_URL`, `CLICKHOUSE_URL`, `REDIS_URL` (+ users, passwords, `*_POOL_SIZE`) | localhost | store connections |
| `SOLACE_HOST`, `SOLACE_PORT`, `SOLACE_VPN`, `SOLACE_USERNAME`, `SOLACE_PASSWORD`, `SOLACE_TOPIC`, `SOLACE_QUEUE` | localhost defaults | Solace transport |

Every store client has bounded waits (Hikari connection timeout, pgjdbc socket timeout
and `statement_timeout`, ClickHouse socket and `max_execution_time`, Jedis pool, connect
and socket timeouts), so an outage is a prompt 503. SIGTERM drains in-flight requests
for `SHUTDOWN_GRACE_PERIOD` (20 s).

## Schema migrations

- **Postgres:** Flyway, `src/main/resources/db/postgres/V<n>__<name>.sql`, applied at
  startup. Flyway holds an advisory lock while migrating, so replicas starting together
  are safe (`MigrationsTest` races six migrators on a fresh database).
- **ClickHouse:** `db/clickhouse/V1.sql`, `V2.sql`, ... applied at startup by
  `ClickHouseMigrations`, recorded in `schema_migrations`. ClickHouse has no DDL lock,
  so every migration must be idempotent (`IF NOT EXISTS`).

## Streaming ingestion

Arrow IPC batches (columns `id` int64 and `op` utf8 in `{upsert, delete}`, both
non-null; `name`, `value` nullable utf8) land in the LSM store served from
`/data/cache`. HTTP decodes and validates every batch of a stream before writing any,
and answers 413 when a batch's decoded Arrow buffers exceed `max-batch-bytes`. All
decodes share one Arrow allocator bounded by `decoder-memory-bytes`.

The LSM store is append-only and bounded by `lsm-max-rows`: the oldest rows are evicted,
counted (`lsm_evicted_rows_total`, `rows_evicted_total` on the ingest health) and logged
at WARN at most once a minute. Compaction is left to the client.

**Solace (`INGEST_TRANSPORT=solace`).** `SolaceBatchConsumer` binds a durable exclusive
queue (`SOLACE_QUEUE`, one per replica), created on start with the topic subscription,
and hands each message's raw payload to `StreamIngestService`, which settles it exactly
once after the write:

| Situation | Outcome |
|---|---|
| Written to the store | ACCEPTED |
| Store write or decode memory failed | FAILED: redelivered after a jittered, doubling backoff (`retry-base-delay-seconds` to `retry-max-delay-seconds`) |
| Over `max-batch-bytes` (checked before decoding), not Arrow IPC, or invalid | REJECTED: never redelivered |

Outcomes are counted in `ingest_outcomes_total{outcome=accepted|failed|rejected}`, which also counts HTTP `POST /data/ingest` requests (accepted, or rejected on a 4xx decode error). `ingest_connection_state{transport,state}` is 1 for the transport's current state (`connected`, `reconnecting`, `down`). Auto-created queues have
unlimited redelivery and no dead-message queue; production queues should set both.

The consumer connects lazily inside the ingest loop, so the app comes up live with
ingest down if the broker is unreachable. Two independent limits then exit the process
(`System.exit(3)`, so an orchestrator restarts it): `max-consecutive-failures` transport
failures in a row without a successful bind (default 5), and the watchdog when the
transport stays disconnected for `max-disconnect-seconds` (default 60). Setting either
to 0 disables only that one. With `noop`, ingest never affects readiness.

## Run with Docker Compose (HTTPS on 443)

```bash
docker compose up -d --build --wait
curl -fsk https://localhost/health/ready
```

The image listens on 8443 (compose publishes `443:8443`), runs as uid 10001 with a
`HEALTHCHECK`, and sizes its heap from the container limit. It carries no TLS key:
compose's one-shot `certs` service writes a self-signed dev keystore into a volume
mounted at `/app/certs`; real deployments mount their own. Compose opts into
`INGEST_TRANSPORT=solace` and starts the broker plus a demo publisher
(`ExampleSolacePublisher`, built from the test sources with the Dockerfile's
`publisher` target, so it never ships in the app jar).

## Adding things

- **An endpoint:** a `@Controller` in `controller/` with `@ExecuteOn(TaskExecutors.BLOCKING)`,
  `@Tag`/`@Operation` for OpenAPI, `@Serdeable` record DTOs in `dto/` (camelCase in
  Java, snake_case on the wire automatically), logic in a `service/` singleton. Throw
  `HttpStatusException` for 4xx; errors render as `{"detail"}` without extra code. Add
  its raw-JSON shape to `ContractTest`.
- **A store:** its client factory with explicit timeouts under `persistence/`, migrations
  if it has a schema, one `PingProbe` method in `health/DependencyProbes` (health,
  readiness, `/metrics` and the startup smoke test pick it up), and an outage mapping in
  `controller/StoreUnavailableHandlers` if its client throws its own exception types.
  Test with a Testcontainer in `support/IntegrationSupport`.
- **An MCP tool:** a `@Tool` method in `mcp/McpTools` that calls the same service as
  the REST endpoint it mirrors; return a DTO (serialised as snake_case JSON) or, for a
  not-found style result, a `CallToolResult` with `isError` (a thrown exception becomes
  a JSON-RPC protocol error). Test it with the SDK client in `McpEndpointTest`.

## Deliberate differences from the Python and Rust templates
1. **Append-only LSM store**: `/data/cache` rows carry `seqno` and `op`; the client compacts.
2. **Stateless MCP transport** (micronaut-mcp's stateless Streamable HTTP: JSON responses,
   no sessions); Python and Rust are session-based.
3. **Lazy Solace connect** (above); Python connects in its lifespan and fails startup.
4. **Solace batch limit** is checked on the encoded payload before decoding; HTTP checks
   the decoded Arrow buffers per batch, like the other templates.
5. **`memory_rss_bytes` / `system_boot_seconds`** come from `/proc` and are `null`
   off Linux.

## Performance profiling
See [PERF_PROFILING_RUNBOOK.md](PERF_PROFILING_RUNBOOK.md): Layer 1 (k6 + Server-Timing)
and Layer 2 (async-profiler).
