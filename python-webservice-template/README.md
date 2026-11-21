# Python Web-Service Template

A production-shaped **FastAPI** service that wires up the technologies a real
enterprise data service needs (ClickHouse, Postgres, Redis, streaming ingest)
behind clean, testable boundaries. It contains **no business logic**. Its job is
to be a *reference*: a place to copy proven boilerplate, and a place to read when
you want to see what "good" looks like for testability, observability and
resilience.

This document is a **map of the example features**: what each one is, why it is
here, and its **entry point in the code**.

- New to the repo and want to run it? → [GETTING_STARTED.md](GETTING_STARTED.md)
- Patterns, commands and recipes for working on it → [CLAUDE.md](CLAUDE.md)

---

## Table of Contents

1. [Core Framework](#1-core-framework)
2. [Wire Contract](#2-wire-contract)
3. [Backing Technologies](#3-backing-technologies)
4. [Streaming Ingestion](#4-streaming-ingestion)
5. [Observability](#5-observability)
6. [Resilience](#6-resilience)
7. [Security](#7-security)
8. [Testing](#8-testing)
9. [Performance & Profiling](#9-performance--profiling)
10. [Configuration and Dependencies](#10-configuration-and-dependencies)
11. [Project Layout](#11-project-layout)

---

## 1. Core Framework

The heart of the service is the **application factory** in [main.py](main.py).
`create_app(settings)` builds a *fully isolated* app (its own services, MCP
server and lifespan), so several apps can run side by side in one process; this
is what lets the whole test suite run in one pytest session.

| Feature | What it does | Entry point |
| --- | --- | --- |
| **App factory + lifespan** | Opens, smoke-tests and migrates every store before serving traffic, then builds the services | [`create_app` / `create_lifespan`](main.py) |
| **Pydantic settings** | All config is a typed `BaseSettings` model; env vars / `.env` override defaults; secrets use `SecretStr` | [settings.py](settings.py) |
| **Dependency injection** | The lifespan stores a frozen `AppServices` dataclass on `app.state.services`; typed `Annotated` getters hand each route its service | [services/app_services.py](services/app_services.py), [core/dependencies.py](core/dependencies.py) |
| **REST-first routers** | Every capability (data, config, cache, health, metrics) is a REST endpoint. Routers are thin; logic lives in services | [routers/](routers/) |
| **Pydantic schemas** | Request/response models give validation and OpenAPI docs (`/docs`) | [schemas/](schemas/) |
| **MCP server** | `FastMCP` mounted at `/mcp`. Its tools mirror the REST read endpoints by calling the same service methods: `get_health_status`, `get_config`, `get_cache`, `get_data`, `get_cached_data`. Each returns the same JSON body as its REST endpoint, as one text content item (a missing cache key gives `isError: true` with the REST 404 body) | [mcp_routers/tools.py](mcp_routers/tools.py) |

---

## 2. Wire Contract

Shared with the Java and Rust templates and pinned by tests:

- JSON field names are snake_case (`ttl_seconds`, `latency_ms`).
- Every error is `application/json` `{"detail": <string or list>}`: 400, 401, 404,
  405, 413, 422, 500, 503, including `/mcp` Host/Origin rejections
  ([core/errors.py](core/errors.py)). Internal error text is logged, not returned.
- A store outage on a request path answers 503 `{"detail": "Service Unavailable"}`
  within the client timeouts.
- `GET /data` is ordered by `id`.
- `POST /data/ingest` validates every batch before writing any; a decoded batch
  over `max_ingest_batch_bytes` answers 413.
- `X-Request-ID` is adopted only if it is 1-128 chars of `[A-Za-z0-9._-]`;
  otherwise a new id is generated.
- `/config` and `/cache` answer with or without a trailing slash.

---

## 3. Backing Technologies

Each external dependency is wrapped in an **async context manager** that connects
with backoff and smoke-tests in `__aenter__` and closes in `__aexit__`. The
lifespan enters them in order, so a failed dependency aborts startup (fail fast)
and any shutdown closes them.

| Technology | Role | Client | Service |
| --- | --- | --- | --- |
| **Postgres** (asyncpg, no ORM; pgvector image, `vector` extension enabled) | Transactional config store | [postgres_client.py](persistence/transaction_store/postgres/postgres_client.py) | [services/config.py](services/config.py) |
| **ClickHouse** (clickhouse-connect async) | Analytics / columnar reads | [clickhouse_client.py](persistence/analytics_store/clickhouse/clickhouse_client.py) | [services/data.py](services/data.py) |
| **Redis** (redis-py async, RedisJSON) | Cache store | [redis_client.py](persistence/cache_store/redis/redis_client.py) | [services/cache.py](services/cache.py) |
| **In-memory LSM store** | Hot cache of streamed record batches, queryable over REST | [lsm_store.py](persistence/stream_store/lsm_store.py) | [services/stream_ingest.py](services/stream_ingest.py) |

**Migrations.** Numbered SQL files in [migrations/postgres](migrations/postgres)
and [migrations/clickhouse](migrations/clickhouse) are applied at startup by
[persistence/migrations.py](persistence/migrations.py); each store records what it
applied in `schema_migrations`. Postgres applies all pending files in one
transaction under an advisory lock (replicas starting together apply each version
once). ClickHouse has no transactions, so its files are single idempotent
statements. A fresh database needs nothing but the app.

---

## 4. Streaming Ingestion

A background thread consumes batches from a streaming transport and writes them
into the LSM store; `/data/cache` serves them.

- **Transports.** `noop` (default: no stream, HTTP ingest only) or `solace`
  (Guaranteed messaging). With `solace`, the consumer binds a **durable exclusive
  queue** (`solace_queue`), creating it on start with the `solace_topic`
  subscription ([ingestion/solace/client.py](ingestion/solace/client.py)). One
  queue per replica: the LSM store is a per-instance cache. The compose demo stack
  sets `INGEST_TRANSPORT=solace`.
- **Settle after write.** Each message becomes a [`Delivery`](ingestion/base.py);
  [`StreamIngestService`](services/stream_ingest.py) settles it once its batches
  are in the store:

  | Situation | Outcome |
  |---|---|
  | Written | ACCEPTED (acked) |
  | Store write raised | FAILED: redelivered, with an increasing backoff between redeliveries |
  | Not Arrow IPC / missing key columns / schema conflicting with the store's / over `max_ingest_batch_bytes` (any batch in the message) | REJECTED before any write: never redelivered (DMQ if configured) |

  Auto-created queues use Solace's default unlimited max-redelivery and have no
  dead-message queue; production should configure both.
- **Schema.** The first accepted batch fixes the store's Arrow schema; a later
  batch with an extra column or a different dtype is refused (422 / REJECTED)
  instead of breaking every later flush and query.
- **HTTP ingest.** [`POST /data/ingest`](routers/data.py) takes an Arrow IPC
  stream (decoded off the event loop): 202 when every batch is written, 400 for an
  unreadable body, 413 for an oversized batch, 422 for an unmergeable one, and
  nothing is written unless all batches pass.
- **Recovery after a terminal interruption.** The SDK reconnects forever, so a
  partition is normally healed by the SDK. If it reports a terminal interruption,
  `deliveries()` raises `SolaceServiceInterrupted`; the loop counts a failure,
  backs off (`ingest_retry_*` settings) and the next call connects a fresh
  service. A successful bind resets the failure count, so only failures to bind
  add up to a shutdown (`ingest_max_consecutive_failures`).
- **Deliberate divergences from the Java template.** *Startup:* Python connects to
  Solace with backoff in the lifespan and fails startup if the broker is
  unreachable; Java connects lazily. *Size limit:* Python measures each batch's
  decoded Arrow `nbytes`; Java measures the encoded payload on Solace deliveries
  only.
- **Extension point: message replay.** The durable queue buffers only unacked
  messages; to rebuild the cache after a restart, start the receiver with a replay
  strategy (`with_message_replay`) against a broker replay log.

---

## 5. Observability

| Feature | What it gives you | Entry point |
| --- | --- | --- |
| **Liveness / readiness / status** | `/health/live` (always 200), `/health/ready` (503 if any dependency is down), `/health/status` (full detail incl. system metrics). Probe results are cached for `health_cache_ttl_seconds` so scrapes do not ping every store each time | [routers/health.py](routers/health.py), [services/health.py](services/health.py) |
| **Prometheus metrics** | `/metrics`: dependency up/latency, ingest state and freshness, counters `ingest_rows_ingested_total` and `ingest_outcomes_total{outcome=accepted\|failed\|rejected}`, `app_process_*` and `system_*` gauges, the standard `process_*`/`python_*` collectors, request histograms | [routers/metrics.py](routers/metrics.py), [services/metrics.py](services/metrics.py) |
| **Structured logs** | One JSON object per line (`timestamp`, `level`, `logger`, `message`, `correlation_id`, `exception`), level from `LOG_LEVEL`; uvicorn's access and error logs go through the same handler | [core/logging_config.py](core/logging_config.py) |
| **Correlation id** | Adopts a safe inbound `X-Request-ID` or generates one, stamps every log line, echoes it back; carried across `to_thread` into the store-write thread | [core/correlation.py](core/correlation.py) |
| **Tracing** | OpenTelemetry, off until `OTEL_EXPORTER_OTLP_ENDPOINT` is set: FastAPI instrumentation with an OTLP/HTTP exporter; continues inbound W3C `traceparent` | [core/tracing.py](core/tracing.py) |
| **Server-Timing header** | Each response carries a W3C `Server-Timing` header built from `timed()` boundary samples | [core/boundary_timing.py](core/boundary_timing.py) |
| **Grafana + Prometheus** | `docker compose --profile observability up` adds Prometheus (`:9090`) and a provisioned Grafana dashboard (`:3000`) | [observability/](observability/) |

---

## 6. Resilience

| Measure | Behaviour | Entry point |
| --- | --- | --- |
| **Fail-fast startup** | Stores are connected, smoke-tested and migrated in the lifespan; if one is unreachable after `connect_max_attempts`, startup aborts | [main.py](main.py), [`connect_with_backoff`](core/retry.py) |
| **Bounded store calls** | Explicit timeouts and pool limits on every client (`postgres_*_timeout_seconds`, pool sizes, `redis_*_timeout_seconds`, `redis_max_connections`, `clickhouse_*_timeout_seconds`); an outage answers 503 instead of hanging | [settings.py](settings.py), [core/errors.py](core/errors.py) |
| **Graceful shutdown** | uvicorn waits `shutdown_timeout_seconds` for in-flight requests; the lifespan then closes the ingest thread and every client | [main.py](main.py) |
| **Ingest self-recovery** | SDK reconnects; unsettled messages are redelivered; after a terminal interruption the loop reconnects with backoff; repeated bind failures request a graceful `SIGTERM` | [`_ingest_loop`](services/stream_ingest.py) |
| **Disconnect watchdog** | If the transport stays non-connected longer than `ingest_max_disconnect_seconds`, the app shuts itself down rather than serve stale data | [`_disconnect_watchdog`](services/stream_ingest.py) |
| **Request body limit** | Outermost middleware answers 413 for an oversized body (fast path on `Content-Length`; byte counting for chunked uploads), even when a mounted app such as MCP swallows the error | [core/request_limits.py](core/request_limits.py) |
| **Decoded batch limit** | `max_ingest_batch_bytes`, checked for every batch before any is written: 413 over HTTP, REJECTED over Solace | [services/stream_ingest.py](services/stream_ingest.py) |

---

## 7. Security

- **Authentication seam, off by default.** `AUTH_ENABLED=true` with
  `AUTH_JWT_SECRET` (HS256) and optional `AUTH_JWT_ISSUER` / `AUTH_JWT_AUDIENCE`
  requires `Authorization: Bearer <jwt>` on every route including `/mcp`, except
  `/health/live` and `/health/ready`. Failure is 401 `{"detail": "Not authenticated"}`
  with `WWW-Authenticate: Bearer`; startup fails if auth is on without a secret.
  Pure-ASGI middleware: [core/auth.py](core/auth.py). Authorisation (who may do
  what) is left to the project.
- **Secrets** are `SecretStr`, never logged; health probes return the generic
  `"unavailable"` / `"timeout"` tokens, never exception text.
- **CORS** defaults to `*` for local development; a validator forbids
  `credentials=True` with `origins=["*"]`.
- **`/mcp` DNS-rebinding protection** from `mcp_allowed_hosts` (JSON array or
  comma-separated).
- **Container**: runs as uid 10001 on port 8443; the TLS key is mounted at runtime
  (compose provisions a dev certificate into a volume with its one-shot `certs`
  service), never baked into an image layer; tests, coverage and editor state are excluded
  from the build context.

---

## 8. Testing

- **Real dependencies, not mocks.** Postgres, ClickHouse, Redis and Solace run in
  `testcontainers`; setup in [tests/conftest.py](tests/conftest.py). Images are
  pinned in [tests/support/images.py](tests/support/images.py).
- **Real HTTP endpoints.** Tests drive the app through an async HTTPX client
  against the real ASGI app ([tests/app_client.py](tests/app_client.py)
  `lifespan_test_client` runs the full lifespan and fails fast if startup raises).
- **Async, parallel across workers.** Tests are `async` (pytest-asyncio, one session
  loop per worker) and run under pytest-xdist (`-n auto`, at most 2 workers). Each
  worker starts its own session-scoped Postgres, ClickHouse and Redis; tests that
  use the Solace broker share one worker. `pytest -n 0` runs serially for debugging.
- **Multiple apps in one process.** Every fixture builds its own app with
  `create_app`, so noop, HTTP-ingest and Solace apps coexist in one session.
- **Failure paths with real outages.** Dedicated containers are stopped or
  paused mid-test: readiness flips to 503 with a generic error
  ([tests/test_observability.py](tests/test_observability.py)), and a request on
  a paused Redis answers a bounded 503 ([tests/test_store_outage.py](tests/test_store_outage.py)).
- **Configured by `Settings`, not monkeypatching.**

```bash
pytest tests/ -v --cov --cov-report=html   # coverage gate in pyproject.toml
```

---

## 9. Performance & Profiling

- **k6** scripts in [tests/performance/](tests/performance/): `smoke.js` (1 VU /
  30 s, CI gate), `load.js` (hard gate), `stress.js` (soft gate); shared checks
  and SLO presets in [lib/](tests/performance/lib/).
- **Two-layer bottleneck attribution** ([runbook](PERF_PROFILING_RUNBOOK.md)):
  the `Server-Timing` header parsed by `profile_reads.js` / `profile_ingest.js`,
  and py-spy flamegraphs on demand
  ([profile/run_pyspy.sh](tests/performance/profile/run_pyspy.sh) +
  [docker-compose.profiling.yml](docker-compose.profiling.yml)).

---

## 10. Configuration and Dependencies

Everything is configured through [settings.py](settings.py); override any field
with an environment variable or `.env` (see [.env.example](.env.example)).
Groups: connections and timeouts (`postgres_*`, `clickhouse_*`, `redis_*`,
`solace_*`), TLS and server (`ssl_*`, `server_*`, `shutdown_timeout_seconds`),
retry (`connect_*`, `ingest_retry_*`), observability (`log_level`,
`otel_*`, `metrics_enabled`, `health_*`, `ingest_staleness_*`,
`ingest_max_disconnect_seconds`), size limits, CORS, auth (`auth_*`).

**Dependency lock.** The standard pip-tools pair: [requirements.in](requirements.in)
holds bounded ranges, and [requirements.txt](requirements.txt), compiled from it,
pins every runtime package exactly and is what the image and CI install (and what
OSV, Trivy and Dependabot read). It targets Linux / Python 3.12 (the image and CI),
so regenerate it in that environment after changing `requirements.in`:

```bash
docker run --rm -v "$PWD:/src" -w /src python:3.12.16-slim sh -c \
  "pip install -q 'pip-tools>=7.4,<8.0' && pip-compile --quiet --strip-extras --no-emit-index-url --output-file=requirements.txt requirements.in"
```

---

## 11. Project Layout

```
main.py                  App factory + lifespan (wiring, fail-fast startup)
settings.py              Typed configuration (Pydantic BaseSettings)
core/                    Cross-cutting infra: auth, errors, correlation, logging,
                         tracing, retry, body limit, Server-Timing, last request
routers/                 REST endpoints (thin): health, data, config, cache, metrics
mcp_routers/tools.py     MCP tools mirroring the REST read endpoints
schemas/                 Pydantic request/response models
services/                Business logic + AppServices (the per-app service set)
persistence/             One package per store (postgres, clickhouse, redis, lsm) + migrations runner
migrations/              Numbered SQL migrations per store
ingestion/               Streaming transports (solace, noop) behind BatchConsumer
observability/           Prometheus config + Grafana dashboards & provisioning
certs/                   Dev certificate generator for `python main.py` (key/cert are gitignored)
tests/                   Pytest integration tests; performance/ holds k6 scripts
PERF_PROFILING_RUNBOOK.md  Two-layer performance profiling guide
```

## Where to Start Reading

1. [settings.py](settings.py): what is configurable.
2. [main.py](main.py): how everything is wired and started.
3. [services/health.py](services/health.py): how the service reports on itself.
4. [tests/test_observability.py](tests/test_observability.py): how that behaviour
   is proven with real dependencies.
