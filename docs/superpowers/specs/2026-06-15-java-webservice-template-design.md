# Java Webservice Template — Design

**Date:** 2026-06-15
**Status:** Approved design (pre-plan)
**Goal:** Create `java-webservice-template`, a feature-for-feature mirror of
`python-webservice-template` built with Micronaut and Java idioms, such that a
developer can read either codebase and mentally map one onto the other.

---

## 1. Guiding principle

The Java template reproduces the Python template's **endpoints, request/response
models, and behaviour** exactly, and mirrors its **structure and naming**
wherever doing so does not force un-idiomatic Java. Where the two languages
genuinely diverge (dependency injection, dataframe libraries, async model), the
Java side uses the idiomatic Java approach and a short comment notes the mapping
back to the Python original.

No business logic is added. Scope equals the Python template's scope — no more.

## 2. Locked technology decisions

| Concern | Python template | Java template |
|---|---|---|
| Web framework | FastAPI | Micronaut 4.x |
| Language / runtime | CPython 3.12 | **Java 25 LTS** |
| Concurrency | async/await | **Virtual threads + blocking** — controllers run `@ExecuteOn(TaskExecutors.VIRTUAL)`, all clients are blocking |
| Build | pip / requirements.txt | **Maven**, single module |
| Dependency injection | hand-rolled `core/container.py` | **Micronaut native** `@Singleton` + constructor injection |
| Request/response models | Pydantic models | **Java `record`s + Jakarta Validation** |
| Configuration | Pydantic `BaseSettings` | **`@ConfigurationProperties` + `application.yml`**, env override |
| Postgres / ClickHouse access | raw asyncpg / clickhouse-connect | **Micronaut Data** repositories (JDBC) |
| Redis | redis-py + RedisJSON | **Jedis** (first-class RedisJSON), blocking on virtual threads |
| Arrow / Flight | pyarrow + pyarrow.flight | **Apache Arrow Java** + Arrow Flight Java |
| Stream dataframe merge | polars window function | **removed** — simplified append-only store (see §5) |
| Messaging | solace-pubsubplus | **solace-java (JCSMP)** |
| Metrics | prometheus-fastapi-instrumentator | **Micronaut Micrometer + Prometheus registry** |
| Tests | pytest + testcontainers | **JUnit 5 + Testcontainers-Java + `@MicronautTest`** |
| Perf load tool | k6 | **k6 — reused unchanged** |
| Perf profiler | py-spy | **async-profiler** (JVM wall/CPU flamegraphs) |
| Coverage gate | coverage.py `fail_under` | **JaCoCo** rule at the same threshold |
| TLS | self-signed PEM via `cryptography` | self-signed **PKCS12 keystore via keytool** |

## 3. Dependency-injection mapping (deliberate structural divergence)

Python hand-rolls a `Container` and `core/dependencies.py` getters **because the
language has no built-in DI**. Java does not need this. The Java template uses
Micronaut's native DI:

- `core/container.py` + `core/dependencies.py` → Micronaut `@Singleton` beans +
  constructor injection. No hand-rolled container.
- `main.create_app(settings)` per-instance isolation → Micronaut's
  `ApplicationContext`. In tests, `@MicronautTest` builds an isolated context
  per test class — this is the `create_app` analogue that lets tests with
  different ingest transports coexist.
- Beans depending on live external connections (e.g. `DataService`) are created
  after startup smoke-tests pass, via an `@Context` eager-init bean / startup
  event listener that performs the connect-with-backoff before the bean is
  exposed (mirrors registering singletons inside the lifespan after health
  checks).

A short comment in `Application.java` documents this mapping so a reader coming
from Python knows where the container "went".

## 4. Package / file mapping

Java package root: `com.example.template`. Directory layout mirrors the Python
packages so the two browse side by side.

| Python | Java |
|---|---|
| `main.py` | `Application.java` |
| `settings.py` | `config/AppSettings.java` + `src/main/resources/application.yml` |
| `routers/health.py` | `controller/HealthController.java` |
| `routers/data.py` | `controller/DataController.java` |
| `routers/config.py` | `controller/ConfigController.java` |
| `routers/cache.py` | `controller/CacheController.java` |
| `routers/metrics.py` | (Micronaut management `/metrics` endpoint — config only) |
| `schemas/data.py` | `dto/DataRow.java`, `dto/DataRowsResponse.java`, `dto/CachedDataRow.java` |
| `schemas/config.py` | `dto/ConfigEntry.java`, `dto/ConfigSetRequest.java` |
| `schemas/cache.py` | `dto/CacheEntry.java`, `dto/CacheSetRequest.java` |
| `schemas/health.py` | `dto/health/*.java` (records mirroring each response) |
| `core/correlation.py` | `core/CorrelationIdFilter.java`, `core/Correlation.java` (`timed`) |
| `core/boundary_timing.py` | `core/ServerTimingFilter.java`, `core/BoundarySamples.java` |
| `core/request_limits.py` | `application.yml` `max-request-size` + `core/BodySizeFilter.java` (chunked case) |
| `core/retry.py` | `core/Retry.java` (`connectWithBackoff`) |
| `core/logging_config.py` | `src/main/resources/logback.xml` + MDC config |
| `core/system_metrics.py` | `core/SystemMetrics.java` (`OperatingSystemMXBean`) |
| `services/*.py` | `service/*Service.java` |
| `persistence/transaction_store/postgres` | `persistence/transactionstore/postgres` (Micronaut Data repo + entity) |
| `persistence/analytics_store/clickhouse` | `persistence/analyticsstore/clickhouse` (repo with `@Query`) |
| `persistence/cache_store/redis` | `persistence/cachestore/redis` (Jedis client wrapper) |
| `persistence/stream_store/lsm_store.py` | `persistence/streamstore/LsmStore.java` |
| `ingestion/base.py` | `ingestion/BatchConsumer.java`, `ingestion/ConnectionState.java` |
| `ingestion/flight/client.py` | `ingestion/flight/FlightBatchConsumer.java` |
| `ingestion/solace/client.py` | `ingestion/solace/SolaceBatchConsumer.java` |
| `mcp_routers/{tools,resources,prompts}.py` | `mcp/{Tools,Resources,Prompts}.java` |
| `scripts/*.sql` | `src/main/resources/db/*.sql` (identical SQL) |
| `certs/generate_self_signed_cert.py` | `certs/generate-keystore.sh` (keytool) |
| `tests/` | `src/test/java/com/example/template/...` mirroring |
| `tests/performance/` (k6) | `tests/performance/` — reused as-is, BASE_URL retargeted |

## 5. Endpoints & DTOs (functional parity)

Identical routes, status codes, and query params to Python:

- `GET /health/live` → `LivenessResponse`
- `GET /health/ready` → `ReadinessResponse` (503 when not ready), dependency
  probes for postgres/clickhouse/redis + ingest health
- `GET /health/status` → `DetailedStatusResponse` (app, uptime, dependencies,
  ingest, requests, system snapshot)
- `GET /data?limit=` → `DataRowsResponse` (ClickHouse)
- `GET /data/cache?limit=` → `DataRowsResponse` (LSM store) — **see DTO note below**
- `POST /data/ingest` (Arrow IPC body) → 202
- `POST /config` → `ConfigEntry` (201), `GET /config` → `List<ConfigEntry>`
- `POST /cache` → `CacheEntry` (201), `GET /cache/{key}` → `CacheEntry` (404 if missing)
- `GET /metrics` (Prometheus format)
- `GET /` root info, `/mcp` (MCP), Swagger UI at `/swagger-ui` (Micronaut OpenAPI)

**`/data/cache` DTO divergence (deliberate):** because the Java LSM store no
longer compacts (see §6), its rows carry `seqno` and `op` so the receiving
client can perform compaction itself. The `/data` (ClickHouse) response stays
byte-identical to Python.

```
GET /data/cache  →
{
  "rows": [
    {"id":1,"name":"a","value":"x","seqno":5,"op":"insert"},
    {"id":1,"name":"a","value":"y","seqno":9,"op":"insert"},
    {"id":2,"name":"b","value":"z","seqno":7,"op":"delete"}
  ],
  "total": 3,
  "limit": 10
}
```

This is the only response model that differs from Python, and only on the cache
path. `CachedDataRow` is a distinct record (`id, name, value, seqno, op`);
`DataRow` (id/name/value) is unchanged.

## 6. Simplified LSM store

The Python `LsmStore` is a single-writer log-structured-merge store with a
polars window-function merge (rank by `seqno` over the key partition, drop
delete tombstones during compaction). Java has no polars, and the user has
chosen to **push compaction to the client**.

Java `LsmStore`:

- **Append-only.** `ingest(batch)` decodes the Arrow batch (Apache Arrow Java)
  into row records `(id, name, value, op)`, assigns a monotonically increasing
  `seqno` per row, and appends to an in-memory buffer. No memtable→run→compaction
  lifecycle, no window-function merge, no tombstone reclamation.
- **Single-writer contract preserved.** Exactly one thread (the ingest consumer
  thread) calls `ingest()`. A `volatile` reference to an immutable snapshot list
  is swapped after each append, so readers (`query()`) always see a consistent
  immutable snapshot with no lock — identical thread-safety contract to Python,
  minus the merge.
- **`query(limit)`** returns the raw appended rows in `seqno` order (oldest
  first), each carrying `seqno` and `op`, plus a `total` raw count. The receiving
  client compacts.
- A class-level comment states explicitly: *"Simplified from the Python template:
  the window-function merge/compaction is removed; the client receiving this
  data performs compaction."*

The `max_ingest_batch_bytes` drop-and-log guard is preserved.

## 7. Concurrency & resilience mapping

- **Virtual threads:** controllers and blocking I/O run on virtual threads via
  `@ExecuteOn(TaskExecutors.VIRTUAL)`. Straight-line blocking code reads like the
  Python async version.
- **Fail-fast startup:** `Retry.connectWithBackoff(...)` performs the same
  randomised exponential backoff (base/max delay, 25% jitter, max attempts);
  each dependency is connected and smoke-tested at startup or the process exits.
- **Ingest thread:** a dedicated platform thread runs the consume loop (mirrors
  Python's `threading.Thread`), with consecutive-failure backoff and a
  consecutive-failure-count → `SIGTERM` trigger.
- **Disconnect watchdog:** a scheduled task polls `connectionState()`; non-connected
  longer than `ingest_max_disconnect_seconds` → `SIGTERM`. Both shutdown triggers
  are disabled when the setting is null, exactly as in Python.
- **Flight consumer close/race handling:** the Python client's lock-based
  do_get-outside-lock teardown is reproduced (`ReentrantLock`), so close() never
  blocks on a hung stream and never abandons a reader.

## 8. Observability mapping

- **Correlation ID:** `CorrelationIdFilter` (HTTP `ServerFilter`) adopts inbound
  `X-Request-ID` or generates one, stores it in **SLF4J MDC**, echoes it on the
  response. Logback pattern includes the MDC value. The ingest thread sets its
  own per-batch ID (mirrors Python).
- **Server-Timing:** `ServerTimingFilter` installs a request-scoped boundary
  sample holder; `Correlation.timed(label)` returns an `AutoCloseable` used with
  try-with-resources around each instrumented boundary; the filter renders the
  `Server-Timing` header (same token normalisation and `total` synthesis, so the
  existing k6 parser works unchanged).
- **Metrics:** Micronaut Micrometer with the Prometheus registry, scrape endpoint
  configured at `/metrics`.
- **System snapshot:** `OperatingSystemMXBean` / `Runtime` for CPU, memory,
  uptime — the psutil analogue.

## 9. Testing strategy

- **JUnit 5 + Testcontainers-Java** for Postgres, ClickHouse, Redis. Real
  dependencies, no mocks — same doctrine as Python.
- `@MicronautTest` per test class gives an isolated `ApplicationContext` (the
  `create_app` analogue), so flight / HTTP-ingest / Solace variants run in one
  test run. Test behaviour is driven by config/properties, not monkeypatching.
- HTTP exercised end-to-end via Micronaut `HttpClient` against the embedded
  server (the HTTPX-client analogue), including the `/mcp` path.
- Schema created from the same `db/postgres-init.sql` / `db/clickhouse-init.sql`.
- ClickHouse seed loaded from an Arrow IPC fixture, mirroring the Python fixture.
- One JUnit test per Python test file, same names where possible.
- **k6 scripts reused unchanged**; Server-Timing attribution tables work because
  the header format is preserved.
- **JaCoCo** coverage rule enforces the same threshold as the Python gate.

## 10. MCP

Official `modelcontextprotocol/java-sdk` mounted at `/mcp`, exposing the single
`get_health_status` tool (mirrors Python), with resource/prompt registration
points left as documented stubs.

**Integration risk (handled spike-first):** the MCP Java SDK's HTTP transports
are servlet-oriented while Micronaut runs on Netty. The implementation plan's
**first MCP task is a spike** to confirm the bridge — most likely via the
`micronaut-servlet` runtime or a thin `@Controller`/handler adapter delegating to
the SDK's server session. If a clean bridge proves infeasible, the documented
fallback is to run the MCP endpoint under the servlet runtime. This risk is
called out so the plan front-loads its resolution rather than discovering it late.

## 11. Build, packaging & local stack

- **Maven** single module, Java 25 toolchain.
- **Dockerfile** (eclipse-temurin 25 JRE base) building a runnable jar; keystore
  generated at build time via `certs/generate-keystore.sh` (keytool), HTTPS on
  443 to match the Python container.
- **docker-compose.yml** + **docker-compose.profiling.yml** mirroring the Python
  stack (Postgres, ClickHouse, Redis, Flight test server, app), same
  `python-template`-style project network naming convention so the shared k6
  image/network conventions hold. (Compose project name will be
  `java-template`; network `java-template_default`.)
- **CI**: GitLab CI mirroring the Python pipeline — Maven verify (JUnit +
  JaCoCo gate) and the three k6 jobs as control gates.

## 12. Out of scope (YAGNI)

- No business logic, no extra endpoints beyond the Python set.
- MCP resources/prompts remain stubs.
- No ORM relationships beyond the single config table and the simple ClickHouse
  read; Micronaut Data is used at its simplest.
- No reactive/Mono-Flux code paths.

## 13. Known divergences from Python (summary)

1. **DI is framework-native** (no hand-rolled container).
2. **LSM store is append-only**; compaction pushed to the client.
3. **`/data/cache` rows carry `seqno` + `op`** (only model difference).
4. **Redis via Jedis** rather than the framework-native Lettuce (for RedisJSON).
5. **Profiler is async-profiler** rather than py-spy.

Every other feature is a faithful mirror.
