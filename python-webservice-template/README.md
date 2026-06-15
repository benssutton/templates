# Python Web-Service Template

A production-shaped **FastAPI** service that wires up the technologies a real
enterprise data service needs — ClickHouse, Postgres, Redis, streaming ingest —
behind clean, testable boundaries. It contains **no business logic**. Its intended job is
to be a *reference*: a place to copy proven boilerplate, and a place to read when
you want to see what "good" looks like for testability, observability and
resilience.

This document is a **map of the example features**. Each section says what the
feature is, why it's here, and the **entry point in the code** so you can jump
straight to it and explore.

- New to the repo and want to run it? → [GETTING_STARTED.md](GETTING_STARTED.md)
- Want the architectural rationale and patterns in depth? → [CLAUDE.md](CLAUDE.md)

---

## Table of Contents

1. [Core Framework — FastAPI, Pydantic, REST-first, MCP](#1-core-framework)
2. [Backing Technologies](#2-backing-technologies)
3. [Streaming Ingestion](#3-streaming-ingestion)
4. [Observability](#4-observability)
5. [Resilience](#5-resilience)
6. [Testing](#6-testing)
7. [Performance & Profiling](#7-performance--profiling)
8. [Configuration](#8-configuration)
9. [Project Layout](#9-project-layout)

---

## 1. Core Framework

The heart of the service is the **application factory** in
[main.py](main.py). `create_app(settings)` builds a
*fully isolated* app — its own DI container, its own MCP server, its own
lifespan — so several apps can run side-by-side in one process (this is what lets
the whole test suite run in a single pytest session).

| Feature | What it does | Entry point |
| --- | --- | --- |
| **FastAPI app factory** | Builds an isolated app; lifespan eagerly opens & smoke-tests every dependency before serving traffic | [`create_app` / `create_lifespan`](main.py#L42-L148) |
| **Pydantic settings** | All config is a typed `BaseSettings` model; env vars / `.env` override defaults; secrets use `SecretStr` | [settings.py](settings.py) |
| **Dependency injection** | A small custom `Container` holds singletons per app; route params resolve them via typed `Annotated` aliases | [core/container.py](core/container.py), [core/dependencies.py](core/dependencies.py) |
| **REST-first routers** | Every capability — data, config, cache, health, metrics — is a REST endpoint. Routers are thin; logic lives in services | [routers/](routers/) |
| **Pydantic schemas** | Request/response models give automatic validation + OpenAPI docs | [schemas/](schemas/) |
| **OpenAPI / Swagger** | Tag metadata is co-located with each router and assembled in the factory; docs served at `/docs` | [`openapi_tags`](main.py#L99) |
| **MCP server** | A `FastMCP` server is mounted at `/mcp` as a **starting point** for exposing capabilities to AI agents. One example tool (`get_health_status`) mirrors a REST endpoint by calling the same service; `resources.py` / `prompts.py` are stubs to fill in. MCPs are optional and add no new logic | [mcp_routers/tools.py](mcp_routers/tools.py), [`app.mount("/mcp")`](main.py#L136) |

> **Design note — REST-first, MCP as a starting point.** Application management,
> configuration, observability and functional calls are *all* REST endpoints.
> The MCP layer is a thin adapter intended to let an AI agent call the same
> capabilities by delegating to the services; it never holds business logic of
> its own. It ships as a worked example (one tool) to extend, not a complete
> mirror of every endpoint.

---

## 2. Backing Technologies

Each external dependency is wrapped in an **async context manager** that opens
the connection in `__aenter__` and closes it in `__aexit__`. The lifespan in
[main.py](main.py#L42-L73) enters them in order, so a
failed dependency aborts startup (fail-fast) and a clean/exception shutdown
always closes them.

| Technology | Role | Client (entry point) | Service |
| --- | --- | --- | --- |
| **Postgres** (asyncpg, no ORM) | Transactional config store | [postgres_client.py](persistence/transaction_store/postgres/postgres_client.py) | [services/config.py](services/config.py) |
| **ClickHouse** (clickhouse-connect async) | Analytics / columnar reads | [clickhouse_client.py](persistence/analytics_store/clickhouse/clickhouse_client.py) | [services/data.py](services/data.py) |
| **Redis** (redis-py async, RedisJSON) | Cache store | [redis_client.py](persistence/cache_store/redis/redis_client.py) | [services/cache.py](services/cache.py) |
| **In-memory LSM store** | Hot cache of the streamed record batches, queryable over REST | [lsm_store.py](persistence/stream_store/lsm_store.py) | [services/stream_ingest.py](services/stream_ingest.py) |

SQL is kept as plain DDL/DML files — the single source of truth shared between
the app, docker-compose and the tests:
[scripts/postgres-init.sql](scripts/postgres-init.sql),
[scripts/clickhouse-init.sql](scripts/clickhouse-init.sql).

---

## 3. Streaming Ingestion

A dedicated background thread consumes batches from a streaming transport and
writes them into the LSM store; the data is then served from `/data/cache`.

- **Pluggable transport** chosen by one setting (`ingest_transport`): Apache
  **Arrow Flight** or **Solace**. Both implement the same
  [`BatchConsumer` protocol](ingestion/base.py).
  - Flight: [ingestion/flight/client.py](ingestion/flight/client.py)
  - Solace: [ingestion/solace/client.py](ingestion/solace/client.py)
- **Ingest loop, reconnection & shutdown logic**:
  [services/stream_ingest.py](services/stream_ingest.py#L85-L140)
  — per-batch correlation IDs, randomised exponential backoff on failure, a
  consecutive-failure limit, and a disconnect watchdog (see
  [Resilience](#5-resilience)).
- **HTTP ingest** path for pushing a batch directly:
  [`POST /data/ingest`](routers/data.py#L37-L51).

---

## 4. Observability

The application exposes the data points needed for monitoring and debugging.

| Feature | What it gives you | Entry point |
| --- | --- | --- |
| **Liveness / readiness / status** | `/health/live` (always 200), `/health/ready` (503 if any dependency down), `/health/status` (full detail incl. system metrics) | [routers/health.py](routers/health.py), [services/health.py](services/health.py) |
| **Prometheus metrics** | `/metrics` exposes dependency up/latency, ingest connection-state & freshness, process & host gauges, request histograms | [routers/metrics.py](routers/metrics.py), [services/metrics.py](services/metrics.py) |
| **Correlation middleware** | Adopts or generates an `X-Request-ID`, stamps every log line via a logging filter, echoes it back. Propagates across `to_thread` into the store-write thread | [core/correlation.py](core/correlation.py) |
| **Server-Timing header** | Each response carries a W3C `Server-Timing` header built from `timed()` boundary samples — per-request latency attribution with no external tooling | [core/boundary_timing.py](core/boundary_timing.py), [`timed()`](core/correlation.py#L74-L87) |
| **System metrics** | Process & host CPU / memory / threads / open files via psutil | [core/system_metrics.py](core/system_metrics.py) |
| **Grafana + Prometheus stack** | `docker compose --profile observability up` brings up Prometheus (`:9090`) scraping `/metrics` and a pre-provisioned Grafana dashboard (`:3000`) | [observability/](observability/), [docker-compose.yml](docker-compose.yml#L142-L165) |

---

## 5. Resilience

Enterprise-grade stability through fail-fast startup and self-recovery.

| Measure | Behaviour | Entry point |
| --- | --- | --- |
| **HTTPS / TLS** | Uvicorn serves over TLS using a key/cert pair; a script generates a self-signed pair for local dev | [`ssl_keyfile`/`ssl_certfile`](settings.py#L19-L20), [certs/generate_self_signed_cert.py](certs/generate_self_signed_cert.py) |
| **Fail-fast startup** | Critical dependencies are eagerly connected and smoke-tested in the lifespan; if one cannot be reached after retries, startup aborts and the process exits | [main.py lifespan](main.py#L42-L73) |
| **Reconnect with randomised backoff** | Startup connections retry with exponential delay **+ up to 25% jitter**, capped, for `connect_max_attempts` before giving up | [`connect_with_backoff`](core/retry.py) |
| **Ingest self-recovery** | The ingest loop reconnects with the same jittered backoff; after N consecutive failures it requests a graceful `SIGTERM` shutdown so an orchestrator can restart it | [`_ingest_loop`](services/stream_ingest.py#L85-L121) |
| **Disconnect watchdog** | If the transport stays non-connected longer than `ingest_max_disconnect_seconds`, the app shuts itself down rather than silently serving stale data | [`_disconnect_watchdog`](services/stream_ingest.py#L123-L144) |
| **Max-payload middleware** | Outermost middleware rejects oversized HTTP bodies with **413** (fast-path on `Content-Length`, byte-counting for chunked uploads) — before any handler runs | [core/request_limits.py](core/request_limits.py), [wiring](main.py#L122-L123) |
| **Decoded-batch limit** | A second, independent guard drops over-sized batches *after* Arrow decode and logs at ERROR (still returns 202 so the caller isn't retried into a storm) | [`_record_ingest`](services/stream_ingest.py#L72-L83) |
| **Secret hygiene** | All credentials are `SecretStr` — never printed in `repr`/logs; health probes return generic `"unavailable"` tokens, never raw exception strings | [settings.py](settings.py#L25-L35), [`_probe`](services/health.py#L53-L70) |

---

## 6. Testing

Faithful, comprehensive tests are the core guardrail of this template. The suite
targets **>90% coverage** and follows a few firm rules.

- **Real dependencies, not mocks.** Tests spin up **real** Postgres / ClickHouse
  / Redis containers via `testcontainers`; assertions go through the actual
  clients. Setup: [tests/conftest.py](tests/conftest.py).
- **Real HTTP endpoints.** Tests drive the app through an async HTTPX client
  against a real ASGI app, not by calling functions directly:
  [tests/app_client.py](tests/app_client.py)
  (`lifespan_test_client` runs the full lifespan, fails fast if startup raises).
- **Async & parallel.** Tests are `async` and run concurrently to keep the cycle
  short ([pytest.ini](pytest.ini)).
- **Multiple application instances in one process.** Because every app is built
  by `create_app`, the Flight, HTTP-ingest and Solace fixtures each get their own
  isolated app — "run all tests" works from the IDE. See the multi-app
  observability tests in
  [tests/test_observability.py](tests/test_observability.py).
- **Failure-path coverage.** Resilience is tested by killing dedicated
  containers mid-test and asserting readiness flips to 503 with a *generic* error
  (e.g. `test_dependency_down_fails_readiness`,
  `test_health_probe_error_is_generic_not_leaky` in
  [tests/test_observability.py](tests/test_observability.py)).
- **Configured by `Settings`, not monkeypatching.** Test behaviour is set by the
  `Settings` passed to `create_app` — no patching, no DI overrides.

Run them:

```bash
pytest tests/ -v --cov --cov-report=html   # report at htmlcov/index.html
```

---

## 7. Performance & Profiling

- **k6 load tests** live in
  [tests/performance/](tests/performance/) and double
  as CI quality gates:
  - `smoke.js` — 1 VU / 30 s, hard gate
  - `load.js` — ramping + constant VUs, hard gate
  - `stress.js` — ramping arrival rate, soft gate
  - Shared check helpers and named SLO presets in
    [lib/](tests/performance/lib/).
- **Two-layer bottleneck attribution** (see the
  [runbook](PERF_PROFILING_RUNBOOK.md)):
  - *Layer 1 (always on, report-only):* the `Server-Timing` header is parsed by
    [profile_reads.js](tests/performance/profile_reads.js)
    / [profile_ingest.js](tests/performance/profile_ingest.js)
    into a ranked per-endpoint attribution table.
  - *Layer 2 (on demand):* attach **py-spy** to the running container for
    flamegraphs that separate CPU/GIL- from I/O-bound time —
    [profile/run_pyspy.sh](tests/performance/profile/run_pyspy.sh)
    + [docker-compose.profiling.yml](docker-compose.profiling.yml).

---

## 8. Configuration

Everything is configured through [settings.py](settings.py)
(Pydantic `BaseSettings`). Defaults are sane for local dev; override any field
with an environment variable or a `.env` file. Notable groups:

- **Connections:** `postgres_*`, `clickhouse_*`, `redis_url`, `flight_*`, `solace_*`
- **TLS:** `ssl_keyfile`, `ssl_certfile`
- **Retry/backoff:** `connect_max_attempts`, `connect_base_delay`, `connect_max_delay`
- **Observability:** `metrics_enabled`, `health_check_timeout_seconds`,
  `ingest_staleness_threshold_seconds`, `ingest_max_disconnect_seconds`
- **Size limits:** `max_request_body_bytes`, `max_ingest_batch_bytes`
- **CORS:** `cors_allow_origins` / `…_methods` / `…_headers` / `…_credentials`
  (a validator forbids the unsafe `credentials=True` + `origins=["*"]` combo)

---

## 9. Project Layout

```
main.py                  App factory + lifespan (dependency wiring, fail-fast startup)
settings.py              Typed configuration (Pydantic BaseSettings)
core/                    Cross-cutting infra: DI container, correlation, retry,
                         request-size + server-timing middleware, system metrics
routers/                 REST endpoints (thin) — health, data, config, cache, metrics
mcp_routers/             MCP starting point — one example tool; resources/prompts are stubs
schemas/                 Pydantic request/response models
services/                Business logic — health, data, config, cache, stream_ingest, metrics
persistence/             One package per backing store (postgres, clickhouse, redis, lsm)
ingestion/               Streaming transports (Arrow Flight, Solace) behind BatchConsumer
scripts/                 SQL DDL/DML (shared by app, compose and tests)
observability/           Prometheus config + Grafana dashboards & provisioning
certs/                   Self-signed TLS cert + generator
tests/                   Pytest integration tests; performance/ holds k6 scripts
docs/                    Design specs and plans
PERF_PROFILING_RUNBOOK.md  Two-layer performance profiling guide (Server-Timing + py-spy)
```

---

## Where to Start Reading

If you have ten minutes, read these four files in order — they show the whole
spine of the template:

1. [settings.py](settings.py) — what's configurable.
2. [main.py](main.py) — how everything is wired and
   started.
3. [services/health.py](services/health.py) — how the
   service reports on itself.
4. [tests/test_observability.py](tests/test_observability.py)
   — how the behaviour above is proven with real dependencies.
