# Python Template

FastAPI service with an MCP mount, wired to Postgres, ClickHouse, Redis and Solace,
with no business logic. [README.md](README.md) maps the features and the wire
contract; this file holds what you need to change the code safely.

## Commands

Repo-root pipeline (p315 interpreter, see root CLAUDE.md):

```bash
python run_CICD.py python fmt          # black + ruff --fix (rewrites files)
python run_CICD.py python fast         # fmt-check + lint + typecheck (seconds)
python run_CICD.py python test         # full pytest suite + coverage gate (needs Docker)
```

From this directory:

```bash
python -m black . && python -m ruff check .     # format, lint
python -m pyright                              # typecheck
pytest tests/test_cache.py::test_post_cache_without_ttl -v   # one test
```

Tests run under pytest-xdist (`-n auto`, at most 2 workers, see `pyproject.toml`): each
worker starts its own Postgres, ClickHouse and Redis; tests that use the Solace
broker share one worker (`xdist_group("solace")`, set in `tests/conftest.py`).
`pytest -n 0` runs serially for debugging. Use `unique()` from
`tests/support/settings.py` for keys.

Never connect to a container a developer may have running; every pytest session
starts its own (Testcontainers) and destroys them afterwards. Do not use
`-o faulthandler_timeout` on the p315 interpreter: CPython 3.15's
`dump_traceback_later` deadlocks between tests; use `py-spy dump --pid` instead.

## Layout

```
main.py            create_app(settings) + lifespan: connect, migrate, build AppServices
settings.py        every setting (env / .env override)
core/              pure-ASGI middleware (body limit, correlation, auth, Server-Timing,
                   last request), errors, logging, tracing, retry, typed dependencies
routers/           thin REST endpoints        services/   logic + AppServices dataclass
schemas/           Pydantic models            mcp_routers/tools.py   MCP tools
persistence/       store clients, LSM store, migrations runner
migrations/        NNN_name.sql per store     ingestion/  solace + noop transports
tests/             pytest (+ support/ helpers, publishers/, performance/ k6)
```

## Key Patterns

**App isolation.** `create_app` builds everything per app (services, FastMCP whose
session manager runs once per instance, lifespan). Nothing app-scoped is
module-level except `app = create_app(get_settings())`. A second app means a
second `create_app` call, never a second lifespan on the same app.

**Services and DI.** The lifespan builds a frozen `AppServices` (services/app_services.py)
once every store is connected and puts it on `app.state.services`. Routes take
`Annotated` aliases from `core/dependencies.py` (`ConfigServiceDep`, ...). MCP
tools get a `lambda: app.state.services` and call the same service methods.
HealthService receives one `ping()` per store and depends only on the stores and
StreamIngestService (for ingest health); it imports no other service.

**Middleware is pure ASGI only.** Never add `BaseHTTPMiddleware` /
`@app.middleware("http")`: it runs `receive()` in a task group, turning the 413
raised mid-read into an ExceptionGroup (FastAPI then answers 400). Order
(outermost first): body limit, correlation, CORS, auth (if enabled), Server-Timing,
last request.

**Errors.** Every error body is `{"detail": ...}`. Raise `HTTPException` in routes;
driver connection/timeout errors (`core/errors.STORE_UNAVAILABLE_ERRORS`) become
503 without any code in the service; anything else becomes a generic 500. The
mounted MCP app's plain-text rejections are rewritten by `JsonErrorBodyMiddleware`.

**Stores.** Each client is an async context manager that connects with backoff,
smoke-tests and (Postgres, ClickHouse) applies migrations in `__aenter__`. All
timeouts and pool sizes are `Settings` fields. Services acquire connections per
operation. Redis must have RedisJSON (probed at startup). The pgvector `vector`
extension is enabled deliberately for future embedding features.

**Migrations.** Add `migrations/<store>/NNN_name.sql`; never edit an applied file.
Postgres runs all pending files in one transaction under the advisory lock
(`SCHEMA_LOCK_ID`). ClickHouse files must be one idempotent
statement each (no transactions or locks there).

**LSM store.** Single logical writer: `ingest()` holds a `threading.Lock` for the
whole write, so HTTP worker threads and the transport thread can both call it.
Readers take no lock (atomic snapshot swap). The first accepted batch fixes the
schema; `validate()` refuses a conflicting one. Tombstones are reclaimed only at
full compaction.

**Ingest policy.** `StreamIngestService._write_all` checks every batch (decoded
size against `max_ingest_batch_bytes`, then `LSMStore.validate`) before writing
any; HTTP maps refusals to 413/422, Solace to REJECTED. Settlement: ACCEPTED after
the write, FAILED (redelivered with backoff) when the write raises, REJECTED for
unreadable, unmergeable or oversized messages. The HTTP body limit
(`max_request_body_bytes`) is separate and enforced by the outermost middleware.

**Solace.** One durable exclusive queue per replica. The SDK reconnects forever;
a terminal interruption makes `deliveries()` raise, the loop backs off and
reconnects with a fresh service. A bind or delivery resets the failure count.
Give-up points: the disconnect watchdog (`ingest_max_disconnect_seconds`) and
`ingest_max_consecutive_failures`. Broker-side SEMP disables do not cause a
terminal interruption under forever-retry; the reconnect test drives the
listener directly.

**Security.** Credentials are `SecretStr`; health payloads carry only
`"unavailable"`/`"timeout"`. The JWT seam (`core/auth.py`) is off by default; when
on it covers `/mcp` and exempts only `/health/live` and `/health/ready`.
`/mcp` Host/Origin allow-list comes from `mcp_allowed_hosts`; never rely on
FastMCP's default.

**Testing.** Tests call real endpoints through `tests/app_client.lifespan_test_client`
(lifespan in its own task; fails fast if startup raises) against real containers.
Behaviour is configured through the `Settings` passed to `create_app`: no
monkeypatching, no dependency overrides. `test_client` is the shared noop app;
streaming tests use the session broker plus `solace_names` (unique topic and
queue per test) and `solace_settings`. Outages are simulated with dedicated
containers (stop / `docker pause`). Tests run sequentially in one event loop.

## Recipes

**Add an endpoint.** Schema in `schemas/`; method on the service in `services/`;
route in `routers/<area>.py` using the `...ServiceDep` alias (new router: export
`TAG`/`TAG_METADATA`, include it and its tag in `main.create_app`); test in
`tests/test_<area>.py` through `test_client`. Errors: raise `HTTPException`.

**Add a store.** Client context manager under `persistence/<kind>/<name>/`
(connect with `connect_with_backoff`, smoke-test, timeouts from new `Settings`
fields); a service with a `ping()`; enter the client in the lifespan, add the
service to `AppServices` and its `ping` to the `HealthService` pings; add its
connection/timeout exception types to `STORE_UNAVAILABLE_ERRORS`; a session
container fixture in `tests/conftest.py` with the image pinned in
`tests/support/images.py`; migrations under `migrations/<name>/` if it has a schema.

**Add an MCP tool.** In `mcp_routers/tools.py`, an `@mcp.tool()` that calls the
same service method as the REST route and returns its response model; extend
`test_mcp_tools_mirror_rest_endpoints`.

## Performance

k6 scripts in `tests/performance/` (`smoke.js` is the CI gate) run against the
compose stack (`python-template_default` network, `BASE_URL=https://app:8443`).
Profiling: Server-Timing attribution and py-spy, see PERF_PROFILING_RUNBOOK.md.
