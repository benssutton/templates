# Python Template

A Python FastAPI service with MCP endpoints and Rust extensions, ready for Claude.

This application is intended as an illustration and re-usable template of best practices when creating a REST-first webservice using FastAPI. See the following sections in this document for details of those best practices.

## Architecture
```
main.py                         FastAPI app entry point; lifespan manages DB connections
settings.py                     Pydantic BaseSettings config; env vars override defaults
  certs/                        Self-signed SSL certificates & certificate generation script
  core/                         DI container and dependency getters
  docs/                         Folder for Claude to store specs and plans.  Not used by the application.
  mcp_routers/                  MCP tools, resources and prompts
  persistence/
    analytics_store/
      clickhouse/               ClickHouse via clickhouse-connect async client
    cache_store/
      redis/                    Redis via redis-py async client
    stream_store/
      flight/                   Apache arrow flight client and LSM store of record batches.
    transaction_store/          
      postgres/                 Postgres via asyncpg connection pool

  routers/                      REST endpoints (health, data, config)
  schemas/                      Pydantic request/response models
  scripts/                      SQL DDL for both Postgres and ClickHouse (run at startup / via docker-compose)
  services/                     All business logic (health, data, config)
  tests/                        Pytest integration tests + k6 performance tests
    test_data/                  Binary test fixtures (clickhouse_seed_data.ipc) + fixture generator notebook
    publishers/                 Reusable dummy publishers used by tests and docker-compose (flight_server.py, solace_publisher.py)
    performance/                k6 performance test scripts
      lib/                      Shared k6 check helpers and SLO threshold presets
      data/                     k6 test data (rows_params.json, clickhouse-seed.sh)
```

## Stack
- FastAPI + Pydantic
- asyncpg — Postgres transaction store (direct, no ORM)
- clickhouse-connect[async] — ClickHouse analytics store
- polars / pyarrow — data shaping and Arrow IPC fixtures
- pytest + testcontainers — integration tests against real DB containers
- k6 — performance tests (smoke, load, stress)
- Docker Compose -- full local stack (Postgres, ClickHouse, app)
- GitLab CI -- pytest and k6 as quality gates

## Key Patterns

**App Factory & Dependency Injection**
- `main.create_app(settings)` builds a fully isolated app: its own `Container`, its own `FastMCP` instance, and its own lifespan. Nothing app-scoped lives at module level except `app = create_app(get_settings())` for uvicorn.
- This isolation is load-bearing: the MCP session manager can only `run()` once per instance, so any code path that needs a second app (e.g. tests with a different ingest transport) must call `create_app` again — never re-run a lifespan against an existing app.
- Custom `Container` in `core/container.py` holds singletons, one instance per app, stored on `app.state.container`; `core/dependencies.py` getters resolve it from `request.app.state` and provide `Annotated` type aliases for routes.
- MCP tools run outside FastAPI's request DI, so `mcp_routers.tools.register(mcp, container)` receives the container explicitly and captures it in tool closures.
- Singletons that depend on external connections (e.g. `DataService`) are registered in the lifespan *after* health checks pass, not at container initialisation time.

**Async**
- All I/O is async. Synchronous blocking calls (e.g. `Path.read_text()`) are acceptable outside of async context managers.

**Config**
- `settings.py` uses Pydantic `BaseSettings` -- env vars override defaults, `.env` is auto-loaded.

**Persistence -- Postgres**
- `PostgresClient` in `persistence/transaction_store/postgres/postgres_client.py` mirrors `ClickHouseClient`: async context manager whose `__aenter__` returns a live `asyncpg.Pool`, `__aexit__` closes it.
- In `main.py` the lifespan wraps startup in `async with PostgresClient(settings) as pg_pool:`. `ConfigService` is registered as a singleton holding the pool.
- Schema is in `scripts/postgres-init.sql` (DDL only, `CREATE TABLE IF NOT EXISTS`). The lifespan runs it at startup -- idempotent, no migration tooling needed.
- Services acquire connections per-operation via `async with pool.acquire() as conn:`. No session injection into routes.

**Persistence -- ClickHouse**
- `ClickHouseClient` in `persistence/analytics_store/clickhouse/clickhouse_client.py` is an async context manager class. Use `async with ClickHouseClient(settings) as client:` -- `__aenter__` returns the raw `AsyncClient`, `__aexit__` closes it. No module-level global state.
- In `main.py` the lifespan wraps startup in `async with ClickHouseClient(settings) as ch_client:` so the connection is guaranteed to close on shutdown whether cleanly or via exception.

**Routers**
- Routers implement minimal business logic and call service methods.
- Each router file exports `TAG` and `TAG_METADATA` constants (name + description). `main.py` assembles `openapi_tags` from these exports -- tag metadata is co-located with the router that owns it, not in `Settings`.

**MCP**
- MCP tools, resources and prompts implement minimal logic and call service methods.

**Persistence -- LSM Stream Store**
- `LSMStore` in `persistence/stream_store/lsm_store.py` is a **single-writer** data structure. The ingest consumer thread is the only writer; concurrent calls to `ingest()` produce undefined behaviour.
- Readers (`query()`) are safe from any thread or coroutine: `_publish()` swaps the snapshot reference atomically, so readers always see a consistent, immutable snapshot.
- Delete tombstones are reclaimed only during full compaction (when run count reaches `lsm_compaction_runs`). Until then, a tombstone in the memtable or an un-compacted run shadows older versions of the same key.

**Inbound Size Policy**
- Two independent limits guard against oversized payloads:
  1. **HTTP body limit** (`max_request_body_bytes`, default 16 MiB): enforced by `core/request_limits.MaxBodySizeMiddleware` — the outermost middleware. Returns 413 before the route handler runs (or after body drain for chunked uploads without `Content-Length`).
  2. **Decoded batch limit** (`max_ingest_batch_bytes`, default 16 MiB): enforced in `services/stream_ingest._record_ingest`. A batch that exceeds this after Arrow IPC decoding is dropped and logged at ERROR level; the request still returns 202 so the caller is not retried.
- Both limits are `Settings` fields and can be tuned per deployment or per-test without code changes.

**Security Posture**
- All credential-bearing settings (`postgres_url`, `clickhouse_password`, `redis_url`, `solace_password`) use Pydantic `SecretStr`. Their values are never included in `repr()`, `str()`, or log output. Call `.get_secret_value()` only at the call site that actually needs the raw string.
- Health-probe endpoints (`/health/ready`, `/health/status`) return `error: "unavailable"` for failed dependency checks — never raw exception strings. Full detail is logged server-side at ERROR level.
- CORS defaults to `cors_allow_origins=["*"]` which is appropriate for local development. Tighten per deployment by setting specific origins; `cors_allow_credentials=True` is rejected by a `@model_validator` when `"*"` is in the origins list.
- RedisJSON is a required capability of the Redis instance. The startup smoke-test (`RedisClient._assert_json_module`) probes `JSON.SET` and raises `RuntimeError` if the module is absent, preventing silent data-loss at runtime.

**Testing**
- Tests invoke REST endpoints via an async HTTPX test client.
- Each client fixture builds its own isolated app with `tests/app_client.py::lifespan_test_client(settings)` — the helper calls `main.create_app(settings)`, runs the lifespan in a dedicated task (anyio cancel scopes must enter/exit in one task), and fails fast if startup raises rather than hanging on a readiness event.
- Because every app is isolated (own container, own MCP), the flight, HTTP-ingest, and Solace client fixtures can all run in a single pytest process — "run all tests" works from the IDE.
- Test behaviour is configured via the `Settings` passed to `create_app` (e.g. `status="testing"`, `ingest_transport`); no monkeypatching, no dependency overrides needed.
- Each test session starts fresh testcontainer instances for Postgres and ClickHouse; containers are torn down at session end.
- Postgres schema is created from `scripts/postgres-init.sql` via the `postgres_pool` fixture.
- ClickHouse schema is created from `scripts/clickhouse-init.sql` (single source of truth shared with docker-compose). Seed data is loaded from `tests/test_data/clickhouse_seed_data.ipc` via `client.insert_arrow()`.

**Performance Tests**
- `tests/performance/lib/checks.js` -- shared k6 check helpers (`checkStatus200`, `checkDataCount`, `checkDataRows`).
- `tests/performance/lib/thresholds.js` -- named SLO presets (`STRICT_SLO`, `NORMAL_SLO`, `RELAXED_SLO`) spread into `options.thresholds`.
- Three scripts: `smoke.js` (1 VU/30 s, hard CI gate), `load.js` (ramping-vus + constant-vus, hard gate), `stress.js` (ramping-arrival-rate, soft gate).
- k6 is run via a `tests/performance/Dockerfile` image built in CI -- avoids docker:dind volume-mount issues.
- The docker-compose project is named `python-template` so the network is always `python-template_default`.

**Performance Profiling**
- Two-layer bottleneck triage; see `PERF_PROFILING_RUNBOOK.md`.
- Layer 1 (every run, report-only): `core/boundary_timing.py` emits a W3C `Server-Timing` response header built from `timed()` boundary samples held in a request-scoped `ContextVar`; `tests/performance/profile_reads.js` and `profile_ingest.js` parse it into a ranked per-endpoint attribution table + `attribution.json`.
- Layer 2 (on demand): `tests/performance/profile/run_pyspy.sh` attaches py-spy to the app container (needs `docker-compose.profiling.yml` for the `SYS_PTRACE` cap) and emits flamegraphs that classify CPU/GIL- vs I/O-bound contention.
- Boundary samples are request-scoped, so background work (the streaming ingest thread) never pollutes a request's header and multiple isolated test apps cannot collide.

**SQL Management**
- `scripts/postgres-init.sql` -- DDL only (CREATE TABLE IF NOT EXISTS). Run by the app lifespan at startup and by pytest via `postgres_pool` fixture.
- `scripts/clickhouse-init.sql` -- DDL only (CREATE TABLE). Used by both docker-compose and pytest.
- `tests/performance/data/clickhouse-seed.sh` -- shell script that bulk-loads the Arrow IPC fixture (`clickhouse_seed_data.ipc`) via `clickhouse-client ... FORMAT Arrow`. Used by docker-compose for performance test data.
- docker-compose mounts the ClickHouse scripts as `01-schema.sql` and `02-seed.sh` so ClickHouse runs them in order.

## Database Investigation

When investigating a Postgres-related issue, always start a fresh container via `testcontainers` by running the relevant pytest test:

```bash
pytest tests/test_config.py -v -s
```

When investigating a ClickHouse-related issue, run:

```bash
pytest tests/test_data.py -v -s
```

Never connect to any container a developer may have running locally. Never assume an existing container is safe to query or modify. Do not reuse containers between investigations -- each pytest session starts a clean, isolated container that is destroyed when the session ends.
