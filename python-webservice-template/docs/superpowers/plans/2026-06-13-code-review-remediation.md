# Code-Review Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Address the ten findings from the in-depth code review of `python-webservice-template`: clarify the LSM single-writer contract, add ClickHouse retry/backoff, enforce inbound size limits, reclaim LSM tombstones, harden the Redis dependency, add CORS, thread correlation IDs end-to-end, adopt `SecretStr`, stop leaking internal errors from health probes, and fix a misleading freshness metric.

**Architecture:** All work stays inside `python-webservice-template/`. New cross-cutting infrastructure lives in `core/` (`correlation.py`, `logging_config.py`, `request_limits.py`). Persistence/ingestion clients gain resilience and size guards. `settings.py` grows configuration fields (all with defaults, so module-level `app = create_app(get_settings())` stays valid). 

**Tech Stack:** FastAPI / Starlette, Pydantic v2 + pydantic-settings (`SecretStr`), asyncpg, clickhouse-connect, redis-py (redis-stack / RedisJSON), polars/pyarrow, pytest + pytest-asyncio + testcontainers.

**Testing philosophy (per CLAUDE.md — load-bearing for this plan):**
- **No mocks, no monkeypatch, no fakes.** Tests drive **real HTTP endpoints** against **real testcontainers**, or drive the **real `create_app` lifespan** via `tests/app_client.py::lifespan_test_client` for startup-failure cases (where no endpoint can be hit because the app never starts).
- **Behaviour is configured through `Settings`** passed to `create_app` — e.g. tiny retry budgets, small size limits — never by patching code.
- **Log assertions use pytest's `caplog`** (the real logging system observing real code under a real request), via the shared `cid_caplog` fixture that attaches the production `CorrelationIdFilter`. This is observation, not mocking.
- Reuse the session-scoped `postgres_container`, `clickhouse_container`, `redis_container`, `test_clickhouse_client` fixtures and the `_settings(...)` / `streaming_flight_server` / `_poll_ready` helpers already in `tests/conftest.py` and `tests/test_observability.py`.
- All commands run from the `python-webservice-template/` directory. Commit after each task.

---

## Task 1: Adopt `SecretStr` for credential-bearing settings (Finding #8)

**Files:**
- Modify: `settings.py`
- Modify: `persistence/transaction_store/postgres/postgres_client.py`
- Modify: `persistence/analytics_store/clickhouse/clickhouse_client.py`
- Modify: `persistence/cache_store/redis/redis_client.py`
- Modify: `ingestion/solace/client.py`
- Test: `tests/test_secrets.py` (create)

Rationale: `postgres_url`, `clickhouse_password`, `redis_url`, and `solace_password` are plain `str` today, so they render in `repr(settings)`, tracebacks, and any settings dump. `SecretStr` masks them; call sites read the plaintext via `.get_secret_value()`. Test fixtures pass plain `str` for these fields — `SecretStr` coerces `str` input automatically, so no fixture changes are needed.

Note on testing: a `SecretStr → str` regression is **not** observable over HTTP (no endpoint exposes a DSN), so the precise guard is a structural type assertion — this is a config-contract invariant, not a mock. We additionally exercise the live app to prove no endpoint leaks the real Postgres password.

- [ ] **Step 1: Write the failing test**

Create `tests/test_secrets.py`:

```python
from pydantic import SecretStr

from settings import Settings


def test_credential_fields_are_secretstr():
    # Config-contract invariant (not a mock): guards against a future revert to
    # plain str. No HTTP endpoint exposes the DSN, so this type assertion — not
    # an endpoint check — is what actually catches a regression here.
    s = Settings()
    for field in (s.postgres_url, s.clickhouse_password, s.redis_url, s.solace_password):
        assert isinstance(field, SecretStr)


async def test_secrets_absent_from_live_endpoints(test_client, postgres_container):
    # Defence in depth, exercised against the running app: the real Postgres
    # password must never surface in an observability/config/root response.
    secret = postgres_container.password
    assert secret
    for path in ("/", "/health/status", "/config/"):
        resp = await test_client.get(path)
        assert resp.status_code == 200
        assert secret not in resp.text
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_secrets.py -v`
Expected: FAIL — fields are `str`, `isinstance(..., SecretStr)` is False.

- [ ] **Step 3: Convert the four fields in `settings.py`**

Add the import at the top of `settings.py`:

```python
from pydantic import SecretStr
```

Change the four field declarations to:

```python
    postgres_url: SecretStr = SecretStr("postgresql://appuser:password@localhost:5432/appdb")
```

```python
    clickhouse_password: SecretStr = SecretStr("")
```

```python
    redis_url: SecretStr = SecretStr("redis://localhost:6379/0")
```

```python
    solace_password: SecretStr = SecretStr("admin")
```

- [ ] **Step 4: Update the four call sites to read `.get_secret_value()`**

In `persistence/transaction_store/postgres/postgres_client.py`, inside `__aenter__`:

```python
            lambda: asyncpg.create_pool(
                self._settings.postgres_url.get_secret_value(),
                min_size=self._settings.postgres_pool_min_size,
                max_size=self._settings.postgres_pool_max_size,
            ),
```

In `persistence/analytics_store/clickhouse/clickhouse_client.py`, the `password=` argument:

```python
            password=self._settings.clickhouse_password.get_secret_value(),
```

In `persistence/cache_store/redis/redis_client.py`, the `from_url` call:

```python
            client = aioredis.Redis.from_url(self._settings.redis_url.get_secret_value())
```

In `ingestion/solace/client.py`, the password property in `_connect`'s `props` dict:

```python
            "solace.messaging.authentication.scheme.basic.password":
                self._settings.solace_password.get_secret_value(),
```

- [ ] **Step 5: Run the new test plus the full suite to confirm no regression**

Run: `pytest tests/test_secrets.py tests/test_config.py tests/test_cache.py tests/test_data.py -v`
Expected: PASS (Postgres/Redis/ClickHouse connect through `.get_secret_value()`; no secret in responses).

- [ ] **Step 6: Commit**

```bash
git add settings.py persistence ingestion tests/test_secrets.py
git commit -m "refactor: store credentials as pydantic SecretStr"
```

---

## Task 2: ClickHouse connect with retry/backoff + Settings-driven backoff (Finding #2)

**Files:**
- Modify: `settings.py`
- Modify: `persistence/analytics_store/clickhouse/clickhouse_client.py`
- Modify: `persistence/transaction_store/postgres/postgres_client.py`
- Modify: `persistence/cache_store/redis/redis_client.py`
- Modify: `main.py`
- Test: `tests/test_startup_resilience.py` (create)

Rationale: Postgres and Redis wrap connect in `core.retry.connect_with_backoff` and fold their smoke-test into the connect closure. ClickHouse connects directly and `main.py` raises `RuntimeError` on the first failed ping — no retry. Two changes: (a) make the backoff budget `Settings`-driven so tests can exercise the real lifespan quickly and operators can tune it; (b) fold the ClickHouse ping into the connect closure wrapped in `connect_with_backoff`, mirroring `RedisClient`. The test drives the **real startup lifespan** with a dead ClickHouse and asserts it aborts fast — no monkeypatch.

- [ ] **Step 1: Add Settings-driven backoff fields**

In `settings.py`, under the `# Observability` block (or a new `# Resilience` block), add:

```python
    # Connection retry/backoff (Postgres, Redis, ClickHouse startup)
    connect_max_attempts: int = 5
    connect_base_delay: float = 1.0
    connect_max_delay: float = 30.0
```

- [ ] **Step 2: Write the failing test (real lifespan, real PG + Redis, dead ClickHouse)**

Create `tests/test_startup_resilience.py`:

```python
import time

import pytest

from settings import Settings
from tests.app_client import lifespan_test_client


async def test_clickhouse_unreachable_aborts_startup_after_bounded_retries(
    postgres_container, redis_container,
):
    # Real Postgres + Redis (both connect on the first try); ClickHouse points at
    # a closed port. With a tiny, Settings-driven backoff budget the real lifespan
    # must retry then abort — not hang, not skip the failure.
    pg_port = int(postgres_container.get_exposed_port(5432))
    redis_port = int(redis_container.get_exposed_port(6379))
    settings = Settings(
        postgres_url=f"postgresql://{postgres_container.username}:{postgres_container.password}@localhost:{pg_port}/{postgres_container.dbname}",
        redis_url=f"redis://localhost:{redis_port}/0",
        clickhouse_host="127.0.0.1",
        clickhouse_port=1,                 # nothing is listening here
        connect_max_attempts=3,
        connect_base_delay=0.001,
        connect_max_delay=0.005,
        ingest_max_disconnect_seconds=None,
    )
    start = time.monotonic()
    with pytest.raises(Exception):
        async with lifespan_test_client(settings):
            pass
    assert time.monotonic() - start < 5.0   # retried quickly, did not hang
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest tests/test_startup_resilience.py -v`
Expected: FAIL — `Settings` has no `connect_max_attempts` field, so construction raises (or, once the field exists but isn't wired, the direct `get_async_client` hangs/raises differently and the bounded-time assertion is unreliable).

- [ ] **Step 4: Rewrite `clickhouse_client.py` to use backoff and fold in the ping**

Replace the whole file with:

```python
import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient

from core.retry import connect_with_backoff
from settings import Settings


class ClickHouseClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: AsyncClient | None = None

    async def __aenter__(self) -> AsyncClient:
        async def _connect() -> AsyncClient:
            client = await clickhouse_connect.get_async_client(
                host=self._settings.clickhouse_host,
                port=self._settings.clickhouse_port,
                username=self._settings.clickhouse_user,
                password=self._settings.clickhouse_password.get_secret_value(),
                database=self._settings.clickhouse_database,
            )
            if not await client.ping():           # smoke-test: raises on failure -> retried
                await client.close()
                raise ConnectionError("ClickHouse startup ping returned False")
            return client

        self._client = await connect_with_backoff(
            _connect,
            label="ClickHouse",
            max_attempts=self._settings.connect_max_attempts,
            base_delay=self._settings.connect_base_delay,
            max_delay=self._settings.connect_max_delay,
        )
        return self._client

    async def __aexit__(self, *_: object) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None
```

- [ ] **Step 5: Thread the Settings backoff budget into Postgres and Redis clients**

In `persistence/transaction_store/postgres/postgres_client.py`, the `connect_with_backoff` call becomes:

```python
        self._pool = await connect_with_backoff(
            lambda: asyncpg.create_pool(
                self._settings.postgres_url.get_secret_value(),
                min_size=self._settings.postgres_pool_min_size,
                max_size=self._settings.postgres_pool_max_size,
            ),
            label="Postgres",
            max_attempts=self._settings.connect_max_attempts,
            base_delay=self._settings.connect_base_delay,
            max_delay=self._settings.connect_max_delay,
        )
```

In `persistence/cache_store/redis/redis_client.py`, the `connect_with_backoff` call becomes:

```python
        self._client = await connect_with_backoff(
            _connect,
            label="Redis",
            max_attempts=self._settings.connect_max_attempts,
            base_delay=self._settings.connect_base_delay,
            max_delay=self._settings.connect_max_delay,
        )
```

- [ ] **Step 6: Remove the now-redundant ping/RuntimeError from `main.py`**

In `main.py` inside the lifespan, replace:

```python
            ch_client = await stack.enter_async_context(ClickHouseClient(settings))
            if not await ch_client.ping():
                raise RuntimeError("ClickHouse startup ping failed")
            container.register_singleton(DataService, DataService(ch_client))
```

with:

```python
            # ClickHouseClient pings inside connect_with_backoff, so a live,
            # smoke-tested client is guaranteed here (or startup has aborted).
            ch_client = await stack.enter_async_context(ClickHouseClient(settings))
            container.register_singleton(DataService, DataService(ch_client))
```

- [ ] **Step 7: Run the startup test plus an integration test**

Run: `pytest tests/test_startup_resilience.py tests/test_data.py -v`
Expected: PASS — startup aborts under 5s with a dead ClickHouse; `test_data.py` still connects to the real container.

- [ ] **Step 8: Commit**

```bash
git add settings.py persistence main.py tests/test_startup_resilience.py
git commit -m "feat: Settings-driven connection backoff; ClickHouse startup retries with ping smoke-test"
```

---

## Task 3: Reclaim LSM tombstones during compaction (Finding #4)

**Files:**
- Modify: `persistence/stream_store/lsm_store.py`
- Test: `tests/test_lsm_store.py` (create), `tests/test_http_ingest.py` (extend)

Rationale: `_compact()` keeps the winning row per key — including `op == "delete"` tombstones, which are only filtered at read time, so a deleted key's tombstone lives in the compacted run forever (unbounded memory under delete churn). Because `_compact()` does a *full* merge of every run, after it no older run can still hold the deleted key, so the tombstone has finished shadowing and is safe to drop; a later re-insert arrives with a higher `seqno` and wins regardless.

Note on testing: the **behaviour** (deleted key absent, re-insert wins) is asserted over HTTP in `test_http_ingest.py`. The **memory-reclamation** property is internal — no row count is visible over HTTP — so one focused unit test reads `store._snapshot.runs`. This is white-box observation of a real structure, not a mock.

- [ ] **Step 1: Write the failing behavioural test over HTTP**

Append to `tests/test_http_ingest.py`:

```python
async def test_reinsert_after_delete_wins_over_http(test_client_http: AsyncClient):
    # upsert -> delete -> re-upsert through the real ingest endpoint; the latest
    # write must win even after the delete has been compacted away.
    for value, op in [("v1", "upsert"), ("v1", "delete"), ("v2", "upsert")]:
        batch = make_batch([(500, "rk", value, op)])
        res = await test_client_http.post(
            "/data/ingest",
            content=_serialize_batch(batch),
            headers={"Content-Type": "application/vnd.apache.arrow.stream"},
        )
        assert res.status_code == 202
    row = await _poll_for_value(test_client_http, 500, "v2")
    assert row["value"] == "v2"
```

- [ ] **Step 2: Write the failing unit test for memory reclamation**

Create `tests/test_lsm_store.py`:

```python
from persistence.stream_store.lsm_store import LSMStore
from tests.publishers.flight_server import make_batch


def _run_row_count(store: LSMStore) -> int:
    # Rows physically retained across compacted runs (memtable excluded). The only
    # way to observe tombstone reclamation, which has no HTTP-visible signal.
    return sum(run.height for run in store._snapshot.runs)


def test_compaction_drops_tombstones():
    store = LSMStore(flush_rows=1, compaction_runs=2)   # flush each batch; compact at 2 runs
    store.ingest(make_batch([(1, "a", "v1", "upsert"), (2, "b", "v1", "upsert")]))  # run 1
    store.ingest(make_batch([(2, "b", "v1", "delete")]))                            # run 2 -> compaction

    rows, total = store.query(limit=100)
    assert total == 1
    assert {r["id"] for r in rows} == {1}          # id=2 deleted, absent from reads
    assert _run_row_count(store) == 1              # tombstone for id=2 physically reclaimed
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `pytest tests/test_lsm_store.py tests/test_http_ingest.py -v`
Expected: FAIL on `test_compaction_drops_tombstones` — `_run_row_count` is 2 (tombstone retained). The HTTP re-insert test may already pass (read-time filtering) — that is fine; it is a regression guard.

- [ ] **Step 4: Drop tombstones in `_compact`**

In `persistence/stream_store/lsm_store.py`, replace `_compact`:

```python
    def _compact(self) -> None:
        merged = _merge_frame(tuple(self._runs), self._key_columns)
        if merged is None:
            self._runs = []
            return
        # Full compaction collapses every run into one, so a delete tombstone has
        # finished shadowing older versions and can be reclaimed here. A later
        # re-insert of the same key arrives with a higher seqno and wins anyway.
        merged = merged.filter(pl.col("op") != "delete")
        self._runs = [merged] if merged.height > 0 else []
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/test_lsm_store.py tests/test_http_ingest.py tests/test_flight_cache.py -v`
Expected: PASS — tombstone reclaimed; HTTP re-insert wins; the documented flight result (`id=1→v2, id=2 absent, id=3→v1`) is unchanged.

- [ ] **Step 6: Commit**

```bash
git add persistence/stream_store/lsm_store.py tests/test_lsm_store.py tests/test_http_ingest.py
git commit -m "fix: reclaim LSM delete tombstones during full compaction"
```

---

## Task 4: Fail fast and document the RedisJSON dependency (Finding #5)

**Files:**
- Modify: `persistence/cache_store/redis/redis_client.py`
- Modify: `.env.example`
- Test: `tests/test_startup_resilience.py` (extend)

Rationale: `CacheService` uses `client.json().set/get`, which require the RedisJSON module (redis-stack). The shipped compose/test stack uses `redis/redis-stack-server`, so the default deployment works — but a developer pointing at stock Redis gets a cryptic `ERR unknown command 'JSON.SET'` at the *first cache write*, not at startup. Per the resilience decision ("critical dependencies eagerly smoke-tested on start"), probe `JSON.SET`/`JSON.DEL` inside `_connect` and raise an actionable error if the module is absent. The test drives the **real lifespan** against a real **stock Redis** container and asserts startup fails fast — no mock.

- [ ] **Step 1: Write the failing test (real lifespan, real stock Redis)**

Append to `tests/test_startup_resilience.py`:

```python
from testcontainers.redis import RedisContainer


async def test_stock_redis_without_json_module_aborts_startup(postgres_container):
    # Real Postgres + a real STOCK Redis (no RedisJSON). The real lifespan must
    # fail fast at the Redis smoke-test with an actionable message — before it
    # ever reaches ClickHouse/ingest.
    with RedisContainer("redis:7") as stock:
        pg_port = int(postgres_container.get_exposed_port(5432))
        redis_port = int(stock.get_exposed_port(6379))
        settings = Settings(
            postgres_url=f"postgresql://{postgres_container.username}:{postgres_container.password}@localhost:{pg_port}/{postgres_container.dbname}",
            redis_url=f"redis://localhost:{redis_port}/0",
            connect_max_attempts=1,
            ingest_max_disconnect_seconds=None,
        )
        with pytest.raises(Exception, match="RedisJSON"):
            async with lifespan_test_client(settings):
                pass
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_startup_resilience.py -k redis -v`
Expected: FAIL — `RedisClient` pings only; stock Redis startup succeeds (no `RedisJSON` error), so the lifespan proceeds and later fails elsewhere or hangs.

- [ ] **Step 3: Add the JSON-module smoke-test to `_connect`**

In `persistence/cache_store/redis/redis_client.py`, replace `__aenter__` and add the helper:

```python
    async def __aenter__(self) -> aioredis.Redis:
        async def _connect() -> aioredis.Redis:
            client = aioredis.Redis.from_url(self._settings.redis_url.get_secret_value())
            await client.ping()          # smoke-test: raises if Redis is unreachable
            await self._assert_json_module(client)
            return client

        self._client = await connect_with_backoff(
            _connect,
            label="Redis",
            max_attempts=self._settings.connect_max_attempts,
            base_delay=self._settings.connect_base_delay,
            max_delay=self._settings.connect_max_delay,
        )
        return self._client

    @staticmethod
    async def _assert_json_module(client: aioredis.Redis) -> None:
        """Fail fast if the server lacks the RedisJSON module the cache relies on.

        CacheService uses JSON.SET/JSON.GET; stock Redis returns 'unknown command'
        only at first write. Probing here surfaces the misconfiguration at startup.
        """
        probe_key = "__startup_json_probe__"
        try:
            await client.json().set(probe_key, "$", {"ok": True})
            await client.delete(probe_key)
        except aioredis.ResponseError as exc:
            raise RuntimeError(
                "Redis is reachable but the RedisJSON module is missing. "
                "This template's cache requires redis-stack (e.g. the "
                "redis/redis-stack-server image). Original error: " + str(exc)
            ) from exc
```

Note: `connect_with_backoff` re-raises the final exception after exhausting attempts, so the `RuntimeError("...RedisJSON...")` propagates out of the lifespan as required by the test's `match="RedisJSON"`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_startup_resilience.py -k redis -v`
Expected: PASS — stock Redis aborts startup with the actionable `RedisJSON` message.

- [ ] **Step 5: Confirm the normal redis-stack path still works**

Run: `pytest tests/test_cache.py -v`
Expected: PASS — redis-stack passes the probe and serves cache reads/writes.

- [ ] **Step 6: Document the requirement in `.env.example`**

In `.env.example`, replace the Redis block:

```bash
# ── Redis ──────────────────────────────────────────────────────────────────────
# REQUIRES the RedisJSON module (use redis-stack, e.g. redis/redis-stack-server).
# The cache uses JSON.SET/JSON.GET; stock Redis fails the startup probe fast.
REDIS_URL=redis://localhost:6379/0
```

- [ ] **Step 7: Commit**

```bash
git add persistence/cache_store/redis/redis_client.py .env.example tests/test_startup_resilience.py
git commit -m "feat: fail fast at startup when RedisJSON module is missing"
```

---

## Task 5: Enforce a max inbound HTTP body size (Finding #3, HTTP half)

**Files:**
- Create: `core/request_limits.py`
- Modify: `settings.py`
- Modify: `main.py`
- Test: `tests/test_request_limits.py` (create), `tests/test_http_ingest.py` (extend)

Rationale: `POST /data/ingest` reads the full request body into memory with no cap, an OOM vector. Add a pure-ASGI middleware that counts streamed bytes and aborts with `413` once the limit is exceeded — enforcing the policy for *all* POST routes, even without `Content-Length` (chunked uploads). Both tests issue **real HTTP requests** (the unit test through the real middleware on a minimal ASGI app; the integration test through the full `/data/ingest` endpoint).

- [ ] **Step 1: Add the settings**

In `settings.py`, under a `# Inbound size limits` block:

```python
    # Inbound size limits
    max_request_body_bytes: int = 16 * 1024 * 1024     # 16 MiB; over-limit HTTP body -> 413
    max_ingest_batch_bytes: int = 16 * 1024 * 1024     # 16 MiB; over-limit stream batch -> dropped+logged
```

- [ ] **Step 2: Write the failing test (real HTTP through the middleware)**

Create `tests/test_request_limits.py`:

```python
import pytest
from httpx import ASGITransport, AsyncClient
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route

from core.request_limits import MaxBodySizeMiddleware


def _app(limit: int) -> Starlette:
    async def echo(request):
        body = await request.body()
        return PlainTextResponse(f"got {len(body)}")

    app = Starlette(routes=[Route("/echo", echo, methods=["POST"])])
    app.add_middleware(MaxBodySizeMiddleware, max_bytes=limit)
    return app


@pytest.mark.asyncio
async def test_body_within_limit_passes():
    async with AsyncClient(transport=ASGITransport(app=_app(1024)), base_url="http://t") as c:
        r = await c.post("/echo", content=b"x" * 512)
    assert r.status_code == 200
    assert r.text == "got 512"


@pytest.mark.asyncio
async def test_body_over_limit_returns_413():
    async with AsyncClient(transport=ASGITransport(app=_app(1024)), base_url="http://t") as c:
        r = await c.post("/echo", content=b"x" * 2048)
    assert r.status_code == 413
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest tests/test_request_limits.py -v`
Expected: FAIL — `core.request_limits` does not exist (ImportError).

- [ ] **Step 4: Implement the middleware**

Create `core/request_limits.py`:

```python
"""ASGI middleware enforcing a maximum inbound request body size.

Counts bytes as they stream from the client and aborts with 413 once the limit
is crossed — works even when Content-Length is absent (chunked uploads). Applied
app-wide so every current and future POST route inherits the policy.
"""
from starlette.types import ASGIApp, Message, Receive, Scope, Send


class MaxBodySizeMiddleware:
    def __init__(self, app: ASGIApp, max_bytes: int) -> None:
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # Fast path: trust an explicit, oversized Content-Length and reject early.
        for name, value in scope.get("headers", []):
            if name == b"content-length":
                try:
                    if int(value) > self.max_bytes:
                        await self._reject(send)
                        return
                except ValueError:
                    pass
                break

        received = 0
        too_large = False

        async def counting_receive() -> Message:
            nonlocal received, too_large
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > self.max_bytes:
                    too_large = True
            return message

        started = False

        async def guarded_send(message: Message) -> None:
            nonlocal started
            if too_large and not started:
                await self._reject(send)
                started = True
                return
            if started:
                return
            await send(message)

        await self.app(scope, counting_receive, guarded_send)

    @staticmethod
    async def _reject(send: Send) -> None:
        await send({
            "type": "http.response.start",
            "status": 413,
            "headers": [(b"content-type", b"text/plain; charset=utf-8")],
        })
        await send({"type": "http.response.body", "body": b"Payload Too Large"})
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/test_request_limits.py -v`
Expected: PASS — 512 B passes, 2048 B returns 413.

- [ ] **Step 6: Wire the middleware into `create_app`**

In `main.py`, add the import near the other `core` imports:

```python
from core.request_limits import MaxBodySizeMiddleware
```

In `create_app`, immediately after `app.state.container = container` and before the `_track_last_request` middleware, add:

```python
    app.add_middleware(MaxBodySizeMiddleware, max_bytes=settings.max_request_body_bytes)
```

- [ ] **Step 7: Add an end-to-end ingest 413 test**

Append to `tests/test_http_ingest.py`:

```python
async def test_post_ingest_oversized_body_returns_413(test_client_http: AsyncClient):
    payload = b"\x00" * (17 * 1024 * 1024)   # > default 16 MiB
    res = await test_client_http.post(
        "/data/ingest",
        content=payload,
        headers={"Content-Type": "application/vnd.apache.arrow.stream"},
    )
    assert res.status_code == 413
```

- [ ] **Step 8: Run the ingest suite**

Run: `pytest tests/test_http_ingest.py -v`
Expected: PASS — oversized body rejected with 413; existing ingest tests unaffected.

- [ ] **Step 9: Commit**

```bash
git add core/request_limits.py settings.py main.py tests/test_request_limits.py tests/test_http_ingest.py
git commit -m "feat: enforce max inbound HTTP body size with 413"
```

---

## Task 6: Size-bound streaming ingest batches (Finding #3, streaming half)

**Files:**
- Modify: `services/stream_ingest.py`
- Test: `tests/test_http_ingest.py` (extend)

Rationale: The HTTP cap protects the request path; decoded Arrow batches from Flight/Solace bypass it. Per policy, oversized batches must be dropped and logged rather than ingested. The guard lives in `_record_ingest` — the single chokepoint shared by the streaming thread **and** the `POST /data/ingest` path — so it is fully exercisable over HTTP: with `max_ingest_batch_bytes` set tiny (and the HTTP body cap left large), POST a real batch and assert it is dropped (cache stays empty) and logged.

- [ ] **Step 1: Write the failing test (real HTTP ingest, real containers)**

Append to `tests/test_http_ingest.py` (uses the same session containers + `empty_flight_server` as the module's `test_client_http`):

```python
import logging

from tests.app_client import lifespan_test_client


async def test_oversized_decoded_batch_is_dropped(
    postgres_container, clickhouse_container, test_clickhouse_client,
    redis_container, empty_flight_server, caplog,
):
    pg_port = int(postgres_container.get_exposed_port(5432))
    ch_port = int(clickhouse_container.get_exposed_port(8123))
    redis_port = int(redis_container.get_exposed_port(6379))
    settings = Settings(
        status="testing",
        postgres_url=f"postgresql://{postgres_container.username}:{postgres_container.password}@localhost:{pg_port}/{postgres_container.dbname}",
        clickhouse_host="localhost", clickhouse_port=ch_port,
        clickhouse_user=clickhouse_container.username or "default",
        clickhouse_password=clickhouse_container.password or "",
        clickhouse_database="default",
        redis_url=f"redis://localhost:{redis_port}/0",
        ingest_transport="flight", flight_host="localhost",
        flight_port=empty_flight_server.port, flight_ticket="items",
        lsm_flush_rows=2, lsm_compaction_runs=2,
        max_request_body_bytes=16 * 1024 * 1024,   # large: let the body reach the handler
        max_ingest_batch_bytes=1,                  # tiny: any decoded batch is "oversized"
        ingest_max_disconnect_seconds=None,
    )
    batch = make_batch([(300, "big", "v1", "upsert")])
    async with lifespan_test_client(settings) as client:
        with caplog.at_level(logging.ERROR):
            res = await client.post(
                "/data/ingest",
                content=_serialize_batch(batch),
                headers={"Content-Type": "application/vnd.apache.arrow.stream"},
            )
        assert res.status_code == 202                       # endpoint accepts the request
        body = (await client.get("/data/cache?limit=100")).json()
        assert body["total"] == 0                           # but the batch was dropped
    assert any("exceeds max_ingest_batch_bytes" in r.message for r in caplog.records)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_http_ingest.py -k oversized_decoded -v`
Expected: FAIL — `_record_ingest` ingests unconditionally, so the row appears in `/data/cache` and nothing is logged.

- [ ] **Step 3: Add the size guard to `_record_ingest`**

In `services/stream_ingest.py`, replace `_record_ingest`:

```python
    def _record_ingest(self, batch: pa.RecordBatch) -> None:
        max_bytes = self._settings.max_ingest_batch_bytes
        if batch.nbytes > max_bytes:
            log.error(
                "dropping ingest batch: %d bytes exceeds max_ingest_batch_bytes (%d)",
                batch.nbytes, max_bytes,
            )
            return
        self._store.ingest(batch)
        self._last_batch_at = datetime.now(timezone.utc)
        self._rows_total += batch.num_rows
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_http_ingest.py -k oversized_decoded -v`
Expected: PASS — oversized batch dropped (cache empty) and the error logged.

- [ ] **Step 5: Confirm streaming integration still ingests normal batches**

Run: `pytest tests/test_flight_cache.py -v`
Expected: PASS — normal Flight batches are well under 16 MiB and ingest as before.

- [ ] **Step 6: Commit**

```bash
git add services/stream_ingest.py tests/test_http_ingest.py
git commit -m "feat: drop and log oversized streaming ingest batches"
```

---

## Task 7: Add configurable CORS (Finding #6)

**Files:**
- Modify: `settings.py`
- Modify: `main.py`
- Test: `tests/test_cors.py` (create)

Rationale: No CORS handling exists. Wire Starlette's `CORSMiddleware`, driven by Settings. Per the brainstorming decision the default is permissive (`["*"]`) for out-of-the-box local UI dev, with credentials disabled (Starlette forbids `*` + credentials). The test issues a **real CORS preflight** against the full app. Authn/authz/security headers remain intentionally out of scope (documented in Task 12).

- [ ] **Step 1: Add settings**

In `settings.py`, under a `# CORS` block:

```python
    # CORS — permissive by default for local UI dev; tighten per deployment.
    cors_allow_origins: list[str] = ["*"]
    cors_allow_methods: list[str] = ["*"]
    cors_allow_headers: list[str] = ["*"]
    cors_allow_credentials: bool = False     # must stay False while origins == ["*"]
```

- [ ] **Step 2: Write the failing test (real preflight over HTTP)**

Create `tests/test_cors.py`:

```python
import pytest
from httpx import ASGITransport, AsyncClient

from main import create_app
from settings import Settings


@pytest.mark.asyncio
async def test_cors_preflight_allows_configured_origin():
    # CORS is wired at app construction, before startup — no lifespan/containers
    # needed to exercise a real preflight request end to end.
    app = create_app(Settings(cors_allow_origins=["https://app.example.com"]))
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.options(
            "/data",
            headers={
                "Origin": "https://app.example.com",
                "Access-Control-Request-Method": "GET",
            },
        )
    assert r.status_code == 200
    assert r.headers["access-control-allow-origin"] == "https://app.example.com"
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest tests/test_cors.py -v`
Expected: FAIL — no `access-control-allow-origin` header (KeyError).

- [ ] **Step 4: Wire `CORSMiddleware` in `create_app`**

In `main.py`, add the import:

```python
from fastapi.middleware.cors import CORSMiddleware
```

In `create_app`, after the `MaxBodySizeMiddleware` line, add:

```python
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_allow_origins,
        allow_methods=settings.cors_allow_methods,
        allow_headers=settings.cors_allow_headers,
        allow_credentials=settings.cors_allow_credentials,
    )
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/test_cors.py -v`
Expected: PASS — preflight returns the configured origin.

- [ ] **Step 6: Commit**

```bash
git add settings.py main.py tests/test_cors.py
git commit -m "feat: add configurable CORS middleware"
```

---

## Task 8: Correlation-ID infrastructure — contextvar, logging, middleware (Finding #7, part 1)

**Files:**
- Create: `core/correlation.py`
- Create: `core/logging_config.py`
- Modify: `main.py`
- Modify: `settings.py`
- Modify: `tests/conftest.py` (add the `cid_caplog` fixture)
- Test: `tests/test_correlation.py` (create)

Rationale: There is no request correlation. Add a `ContextVar` carrying a correlation ID, a logging filter that stamps every record, a `configure_logging()` exposing the ID, and an HTTP middleware that adopts an inbound `X-Request-ID` (or generates one) and echoes it on the response. The middleware is verified over **real HTTP**; the filter is verified end-to-end in Task 9 via the shared `cid_caplog` fixture added here (it attaches the production filter to pytest's real log handler — observation, not mocking).

- [ ] **Step 1: Write the failing tests (real HTTP requests)**

Create `tests/test_correlation.py`:

```python
import pytest
from httpx import ASGITransport, AsyncClient

from main import create_app
from settings import Settings


@pytest.mark.asyncio
async def test_middleware_echoes_inbound_id():
    app = create_app(Settings())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.get("/", headers={"X-Request-ID": "trace-42"})
    assert r.headers["X-Request-ID"] == "trace-42"


@pytest.mark.asyncio
async def test_middleware_generates_id_when_absent():
    app = create_app(Settings())
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.get("/")
    assert len(r.headers["X-Request-ID"]) >= 8     # generated UUID hex
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_correlation.py -v`
Expected: FAIL — no `X-Request-ID` response header (KeyError); middleware not wired.

- [ ] **Step 3: Implement `core/correlation.py`**

Create `core/correlation.py`:

```python
"""Request/ingest correlation identity, propagated via a ContextVar.

A single ID is carried for the lifetime of an HTTP request (set by
CorrelationIdMiddleware) or a single ingested batch (set by the ingest loop).
The logging filter stamps every record with the current value so one grep of
the ID surfaces the full causal trail. asyncio.to_thread copies the context, so
the HTTP /data/ingest path carries the request ID into the store-write thread
automatically; the streaming ingest thread sets its own per-batch ID.
"""
import contextvars
import logging
import uuid

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

correlation_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "correlation_id", default="-"
)


def get_correlation_id() -> str:
    return correlation_id_var.get()


def set_correlation_id(value: str) -> None:
    correlation_id_var.set(value)


def new_id() -> str:
    return uuid.uuid4().hex


class CorrelationIdFilter(logging.Filter):
    """Injects the current correlation ID onto every LogRecord."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = correlation_id_var.get()
        return True


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, header_name: str = "X-Request-ID") -> None:
        super().__init__(app)
        self._header = header_name

    async def dispatch(self, request: Request, call_next):
        # Set BEFORE call_next so the value is visible to the downstream endpoint
        # and its loggers (the reliable direction for BaseHTTPMiddleware + contextvars).
        incoming = request.headers.get(self._header)
        cid = incoming or new_id()
        token = correlation_id_var.set(cid)
        try:
            response = await call_next(request)
        finally:
            correlation_id_var.reset(token)
        response.headers[self._header] = cid
        return response
```

- [ ] **Step 4: Implement `core/logging_config.py`**

Create `core/logging_config.py`:

```python
"""Idempotent logging configuration that surfaces the correlation ID.

Installs a stream handler on the root logger whose formatter includes
[%(correlation_id)s], and attaches CorrelationIdFilter so the attribute always
exists. Safe to call multiple times — the multi-app test pattern constructs many
apps in one process.
"""
import logging

from core.correlation import CorrelationIdFilter

_CONFIGURED = False
_FORMAT = "%(asctime)s %(levelname)s [%(correlation_id)s] %(name)s: %(message)s"


def configure_logging(level: int = logging.INFO) -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(_FORMAT))
    handler.addFilter(CorrelationIdFilter())
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(level)
    _CONFIGURED = True
```

- [ ] **Step 5: Add the header-name setting and wire into `main.py`**

In `settings.py`, under the CORS/limits area:

```python
    correlation_id_header: str = "X-Request-ID"
```

In `main.py` add imports:

```python
from core.correlation import CorrelationIdMiddleware
from core.logging_config import configure_logging
```

Make `configure_logging()` the first line of `create_app`:

```python
    configure_logging()
```

Add the middleware **last** among the `add_middleware` calls (outermost — stamps every other layer), after the `CORSMiddleware` block:

```python
    app.add_middleware(CorrelationIdMiddleware, header_name=settings.correlation_id_header)
```

- [ ] **Step 6: Add the `cid_caplog` fixture to `tests/conftest.py`**

Append to `tests/conftest.py`:

```python
@pytest.fixture
def cid_caplog(caplog):
    """caplog with the production CorrelationIdFilter attached, so captured
    records expose `record.correlation_id`. Real logging, no mocking."""
    from core.correlation import CorrelationIdFilter
    caplog.handler.addFilter(CorrelationIdFilter())
    return caplog
```

- [ ] **Step 7: Run test to verify it passes**

Run: `pytest tests/test_correlation.py -v`
Expected: PASS — responses carry `X-Request-ID` (echoed or generated).

- [ ] **Step 8: Commit**

```bash
git add core/correlation.py core/logging_config.py main.py settings.py tests/conftest.py tests/test_correlation.py
git commit -m "feat: end-to-end correlation IDs via contextvar, logging filter, and middleware"
```

---

## Task 9: Per-batch ingest IDs and boundary timing, verified over HTTP/streaming (Finding #7, part 2)

**Files:**
- Modify: `core/correlation.py` (add `timed`)
- Modify: `services/stream_ingest.py` (per-batch ID + ingest log line)
- Modify: `services/data.py`, `services/config.py` (boundary timing)
- Test: `tests/test_http_ingest.py` (HTTP propagation), `tests/test_observability.py` (streaming IDs + timing)

Rationale: With the ID plumbing in place, (a) the HTTP `/data/ingest` request ID must propagate through `asyncio.to_thread` into the store write, (b) each streaming batch must get its own ID so background errors are attributable, and (c) backend boundaries must log durations to locate bottlenecks. All three are proven by driving **real endpoints / a real Flight stream** and observing the resulting logs via `cid_caplog`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_http_ingest.py`:

```python
async def test_http_ingest_propagates_request_id_to_store_write(
    test_client_http: AsyncClient, cid_caplog,
):
    import logging
    batch = make_batch([(201, "http", "v1", "upsert")])
    with cid_caplog.at_level(logging.DEBUG):
        res = await test_client_http.post(
            "/data/ingest",
            content=_serialize_batch(batch),
            headers={
                "Content-Type": "application/vnd.apache.arrow.stream",
                "X-Request-ID": "req-xyz",
            },
        )
    assert res.status_code == 202
    ingest_logs = [r for r in cid_caplog.records if "ingested batch" in r.getMessage()]
    assert ingest_logs and any(r.correlation_id == "req-xyz" for r in ingest_logs)
```

Append to `tests/test_observability.py` (reuses `_settings`, `streaming_flight_server`, `_poll_ready`):

```python
async def test_streaming_assigns_distinct_per_batch_ids(
    postgres_container, clickhouse_container, test_clickhouse_client,
    redis_container, streaming_flight_server, cid_caplog,
):
    import asyncio
    import logging
    settings = _settings(
        postgres_container, clickhouse_container,
        f"redis://localhost:{int(redis_container.get_exposed_port(6379))}/0",
        streaming_flight_server.port, ingest_max_disconnect_seconds=None,
    )
    with cid_caplog.at_level(logging.DEBUG):
        async with lifespan_test_client(settings) as client:
            await _poll_ready(client, 200)
            await asyncio.sleep(0.3)     # let several batches stream in
    ingest_logs = [r for r in cid_caplog.records if "ingested batch" in r.getMessage()]
    cids = {r.correlation_id for r in ingest_logs}
    assert len(ingest_logs) >= 2
    assert "-" not in cids               # every batch got a real ID
    assert len(cids) >= 2                # and a DISTINCT one per batch


async def test_get_data_logs_clickhouse_timing(
    postgres_container, clickhouse_container, test_clickhouse_client,
    redis_container, streaming_flight_server, cid_caplog,
):
    import logging
    settings = _settings(
        postgres_container, clickhouse_container,
        f"redis://localhost:{int(redis_container.get_exposed_port(6379))}/0",
        streaming_flight_server.port, ingest_max_disconnect_seconds=None,
    )
    with cid_caplog.at_level(logging.DEBUG, logger="core.correlation"):
        async with lifespan_test_client(settings) as client:
            resp = await client.get("/data?limit=5")
            assert resp.status_code == 200
    assert any("clickhouse.select" in r.getMessage() for r in cid_caplog.records)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_http_ingest.py -k propagates tests/test_observability.py -k "per_batch or timing" -v`
Expected: FAIL — `timed` is undefined; `_record_ingest` logs nothing ("ingested batch" absent); the loop sets no per-batch ID.

- [ ] **Step 3: Add `timed` to `core/correlation.py`**

Append to `core/correlation.py`:

```python
import time
from contextlib import asynccontextmanager

_timing_log = logging.getLogger(__name__)


@asynccontextmanager
async def timed(label: str):
    """Log the wall-clock duration of an awaited boundary, tagged with the
    current correlation ID (added by CorrelationIdFilter). Structured as a
    context manager so an OpenTelemetry span could wrap the same boundary later
    without changing call sites."""
    start = time.perf_counter()
    try:
        yield
    finally:
        _timing_log.debug("%s %.2fms", label, (time.perf_counter() - start) * 1000)
```

- [ ] **Step 4: Per-batch ID + ingest log line in `services/stream_ingest.py`**

Add the import:

```python
from core.correlation import new_id, set_correlation_id
```

In `_ingest_loop`, set a fresh ID per batch:

```python
                for batch in self._consumer.batches():
                    consecutive_failures = 0
                    delay = _INGEST_BASE_DELAY
                    set_correlation_id(new_id())     # per-batch ID for this thread's logs
                    try:
                        self._record_ingest(batch)
                    except Exception:
                        log.exception("ingest failed; skipping batch")
```

Add the observable success log to `_record_ingest` (its final form, including the Task 6 size guard):

```python
    def _record_ingest(self, batch: pa.RecordBatch) -> None:
        max_bytes = self._settings.max_ingest_batch_bytes
        if batch.nbytes > max_bytes:
            log.error(
                "dropping ingest batch: %d bytes exceeds max_ingest_batch_bytes (%d)",
                batch.nbytes, max_bytes,
            )
            return
        self._store.ingest(batch)
        self._last_batch_at = datetime.now(timezone.utc)
        self._rows_total += batch.num_rows
        log.debug("ingested batch: rows=%d", batch.num_rows)
```

- [ ] **Step 5: Add boundary timing to the backend services**

In `services/data.py`, `from core.correlation import timed`, then wrap the two ClickHouse queries:

```python
    async def get_data(self, limit: int) -> DataRowsResponse:
        async with timed("clickhouse.count"):
            count_result = await self._client.query("SELECT count() FROM items")
        total = count_result.first_row[0]

        async with timed("clickhouse.select"):
            result = await self._client.query(
                "SELECT id, name, value FROM items LIMIT %(limit)s",
                parameters={"limit": limit},
            )
        rows = [
            DataRowResponse(id=row[0], name=row[1], value=row[2])
            for row in result.result_rows
        ]
        return DataRowsResponse(rows=rows, total=total, limit=limit)
```

In `services/config.py`, `from core.correlation import timed`, then wrap `get_all` and `set`:

```python
    async def get_all(self) -> list[ConfigEntry]:
        async with timed("postgres.config.get_all"):
            async with self._pool.acquire() as conn:
                rows = await conn.fetch("SELECT key, value FROM configuration ORDER BY key")
        return [ConfigEntry(key=row["key"], value=row["value"]) for row in rows]
```

```python
    async def set(self, key: str, value: str) -> ConfigEntry:
        async with timed("postgres.config.set"):
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    INSERT INTO configuration (key, value)
                    VALUES ($1, $2)
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                    RETURNING key, value
                    """,
                    key,
                    value,
                )
        return ConfigEntry(key=row["key"], value=row["value"])
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `pytest tests/test_http_ingest.py -k propagates tests/test_observability.py -k "per_batch or timing" -v`
Expected: PASS — request ID reaches the store write; streaming batches get distinct IDs; `clickhouse.select` timing logged.

- [ ] **Step 7: Run the broader suites that exercise these paths**

Run: `pytest tests/test_data.py tests/test_config.py tests/test_flight_cache.py -v`
Expected: PASS — timing/ID wrappers are transparent to behaviour.

- [ ] **Step 8: Commit**

```bash
git add core/correlation.py services/stream_ingest.py services/data.py services/config.py tests/test_http_ingest.py tests/test_observability.py
git commit -m "feat: per-batch ingest IDs and boundary timing logs"
```

---

## Task 10: Stop leaking internal error strings from health probes (Finding #9)

**Files:**
- Modify: `services/health.py`
- Modify: `services/cache.py`, `services/config.py`, `services/data.py`
- Test: `tests/test_observability.py` (extend)

Rationale: `HealthService._probe` and each service's `health_check` put raw `str(exc)` (which can carry host/DSN fragments) into the `/health/ready` response — an unauthenticated endpoint. Log the detail (now correlation-tagged) and return a generic `"unavailable"` reason. The test reuses the existing **dedicated-Redis-stop** pattern: start the real app, `stop()` the dedicated Redis, then read `/health/ready` and assert the generic reason.

- [ ] **Step 1: Write the failing test (real app, real dependency taken down)**

Append to `tests/test_observability.py`:

```python
async def test_health_probe_error_is_generic_not_leaky(
    postgres_container, clickhouse_container, test_clickhouse_client, streaming_flight_server,
):
    dedicated_redis = RedisContainer(REDIS_IMAGE)
    dedicated_redis.start()
    try:
        redis_url = f"redis://localhost:{int(dedicated_redis.get_exposed_port(6379))}/0"
        settings = _settings(
            postgres_container, clickhouse_container, redis_url, streaming_flight_server.port,
        )
        async with lifespan_test_client(settings) as client:
            assert (await _poll_ready(client, 200)).status_code == 200
            dedicated_redis.stop()                 # take down only this test's redis
            resp = await _poll_ready(client, 503, timeout=15.0)
            redis_check = {c["name"]: c for c in resp.json()["checks"]}["redis"]
            assert redis_check["status"] == "down"
            assert redis_check["error"] == "unavailable"      # generic, no host/DSN/driver text
    finally:
        try:
            dedicated_redis.stop()
        except Exception:
            pass
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_observability.py -k generic_not_leaky -v`
Expected: FAIL — `redis_check["error"]` is the raw driver exception string, not `"unavailable"`.

- [ ] **Step 3: Generic error + server-side logging in each service `health_check`**

In `services/cache.py`, add a logger at the top:

```python
import logging

log = logging.getLogger(__name__)
```

and the `except` block of `health_check` becomes:

```python
        except Exception:
            latency_ms = (time.perf_counter() - start) * 1000
            log.warning("redis health check failed", exc_info=True)
            return ProbeResult(
                name="redis", status="down",
                latency_ms=round(latency_ms, 2), error="unavailable",
            )
```

In `services/config.py`, add the same logger and `except` block (name `"postgres"`, message `"postgres health check failed"`).

In `services/data.py` (already has `log`), the `except` block becomes name `"clickhouse"`, message `"clickhouse health check failed"`, `error="unavailable"`. Leave the existing `"ping returned False"` branch as-is (non-sensitive).

- [ ] **Step 4: Generic error in `HealthService._probe`**

In `services/health.py`, the final `except` of `_probe`:

```python
        except Exception:
            log.warning("dependency probe '%s' failed", name, exc_info=True)
            return ProbeResult(name=name, status="down", latency_ms=0.0, error="unavailable")
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/test_observability.py -k generic_not_leaky -v`
Expected: PASS — `error == "unavailable"`; detail only in logs.

- [ ] **Step 6: Confirm healthy probes still report `up`**

Run: `pytest tests/test_health.py tests/test_observability.py -k readiness -v`
Expected: PASS — healthy dependencies unaffected.

- [ ] **Step 7: Commit**

```bash
git add services/health.py services/cache.py services/config.py services/data.py tests/test_observability.py
git commit -m "fix: return generic health-probe errors, log details server-side"
```

---

## Task 11: Distinguish "never ingested" from "just now" in metrics (Finding #10)

**Files:**
- Modify: `services/metrics.py`
- Test: `tests/test_observability.py` (extend, with an empty-Flight fixture)

Rationale: `self.ingest_secs.set(ingest.seconds_since_last_batch or 0.0)` reports `0` both when a batch just arrived and when none ever has, so a freshness alert can't tell a healthy stream from one that never started. Emit `NaN` when `seconds_since_last_batch is None`. Both cases are asserted by scraping the **real `/metrics`** endpoint.

- [ ] **Step 1: Add an empty-Flight fixture + a metric parser, and write the failing tests**

Append to `tests/test_observability.py`:

```python
def _metric_value(text: str, name: str) -> str | None:
    for line in text.splitlines():
        if line.startswith(name + " "):
            return line.split(" ", 1)[1].strip()
    return None


@pytest.fixture(scope="module")
def empty_flight_server():
    location = pa_flight.Location.for_grpc_tcp("localhost", 0)
    server = ExampleFlightServer(location, [], interval=0.0, loop=False)   # connects, sends nothing
    threading.Thread(target=server.serve, daemon=True).start()
    yield server
    server.shutdown()


async def test_metrics_freshness_is_nan_when_never_ingested(
    postgres_container, clickhouse_container, test_clickhouse_client,
    redis_container, empty_flight_server,
):
    settings = _settings(
        postgres_container, clickhouse_container,
        f"redis://localhost:{int(redis_container.get_exposed_port(6379))}/0",
        empty_flight_server.port, ingest_max_disconnect_seconds=None,
    )
    async with lifespan_test_client(settings) as client:
        text = (await client.get("/metrics")).text
    assert _metric_value(text, "ingest_seconds_since_last_batch") == "NaN"


async def test_metrics_freshness_is_finite_after_ingest(
    postgres_container, clickhouse_container, test_clickhouse_client,
    redis_container, streaming_flight_server,
):
    settings = _settings(
        postgres_container, clickhouse_container,
        f"redis://localhost:{int(redis_container.get_exposed_port(6379))}/0",
        streaming_flight_server.port, ingest_max_disconnect_seconds=None,
    )
    async with lifespan_test_client(settings) as client:
        await _poll_ready(client, 200)
        text = (await client.get("/metrics")).text
    value = _metric_value(text, "ingest_seconds_since_last_batch")
    assert value is not None and value != "NaN"
    assert float(value) >= 0.0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_observability.py -k freshness -v`
Expected: FAIL on the never-ingested case — the gauge renders `0.0`, not `NaN`, because of `or 0.0`.

- [ ] **Step 3: Fix the gauge update in `services/metrics.py`**

In `services/metrics.py::refresh`, replace:

```python
        self.ingest_secs.set(ingest.seconds_since_last_batch or 0.0)
```

with:

```python
        # None == no batch ever received: emit NaN so dashboards distinguish
        # "never started" from a batch that arrived 0s ago.
        self.ingest_secs.set(
            ingest.seconds_since_last_batch
            if ingest.seconds_since_last_batch is not None
            else float("nan")
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_observability.py -k freshness -v`
Expected: PASS — `NaN` when never ingested, finite value once batches flow.

- [ ] **Step 5: Confirm the rest of the metrics suite still renders**

Run: `pytest tests/test_observability.py -k metrics -v`
Expected: PASS — `generate_latest` renders `NaN` without error.

- [ ] **Step 6: Commit**

```bash
git add services/metrics.py tests/test_observability.py
git commit -m "fix: emit NaN for ingest freshness when no batch received"
```

---

## Task 12: Documentation — single-writer contract, resilience policy, security posture (Findings #1, #3, #6)

**Files:**
- Modify: `persistence/stream_store/lsm_store.py` (docstrings/comments only)
- Modify: `services/stream_ingest.py` (docstring/comment only)
- Modify: `routers/data.py` (comment only)
- Modify: `python-webservice-template/CLAUDE.md`
- Modify: `CLAUDE.md` (repo root)

Rationale: The LSM store is intentionally single-writer; the Flight/Solace/HTTP ingestion paths are *illustrative alternatives*, not meant to run concurrently against one store. Document that contract where the code lives, record the new inbound-size resilience policy in the root architectural decisions, and state explicitly that authn/authz/security headers are deliberately out of scope.

- [ ] **Step 1: Document the single-writer contract in `lsm_store.py`**

Add directly under `class LSMStore:`:

```python
class LSMStore:
    """Single-writer, lock-free in-memory LSM store.

    CONTRACT: exactly one writer thread calls ingest()/_flush()/_compact() at a
    time. The memtable and runs are writer-private; query() takes a lock-free
    atomic snapshot read that is safe only because there is a single writer.

    The Flight, Solace, and HTTP `/data/ingest` paths are *illustrative,
    mutually-exclusive* ways to attach an ingestion source — they are NOT
    designed to drive one store concurrently. A deployment selects one transport
    (ingest_transport) which owns the single ingest thread; `/data/ingest` is a
    manual/test push for that same logical writer. Driving two sources into one
    store at once would violate this contract and require external locking.
    """
```

- [ ] **Step 2: Reinforce in `services/stream_ingest.py` and `routers/data.py`**

In `services/stream_ingest.py`, set the `StreamIngestService` class docstring:

```python
class StreamIngestService:
    """Owns the single ingest thread that feeds the LSMStore (see LSMStore's
    single-writer contract). The selected transport (Flight or Solace) is the
    one logical writer; HTTP /data/ingest pushes through the same service so the
    store still sees a single writer at a time."""
```

In `routers/data.py`, above the `ingest_batch` route:

```python
# Manual/test push into the SAME logical writer the configured transport owns.
# Not for concurrent use alongside an active streaming transport — see LSMStore's
# single-writer contract.
@router.post("/ingest", status_code=202)
```

- [ ] **Step 3: Update the project `CLAUDE.md`**

In `python-webservice-template/CLAUDE.md`, under `Key Patterns`, add:

```markdown
**Persistence -- Stream store (LSM)**
- `LSMStore` is single-writer and lock-free: one ingest thread owns all writes; `query()` is a lock-free snapshot read valid only under that single-writer contract.
- Flight, Solace and HTTP `/data/ingest` are illustrative, mutually-exclusive ingestion attachments — a deployment picks one transport; they are not meant to write one store concurrently.
- Delete tombstones are reclaimed during full compaction; inbound batches above `max_ingest_batch_bytes` are dropped and logged.
```

- [ ] **Step 4: Update the root `CLAUDE.md`**

In the repo-root `CLAUDE.md`, under `# Architectural Decisions` → `Resilience`, add:

```markdown
2. All inbound ingestion is size-bounded. HTTP request bodies exceeding the configured limit are rejected with 413 (`max_request_body_bytes`); streaming batches exceeding `max_ingest_batch_bytes` are dropped and logged as errors rather than ingested.
```

Add new blocks after `Observability`:

```markdown
Security
1. CORS is provided as configurable middleware. Other cross-origin/security concerns are environment-specific.
2. Authentication, authorization and security headers are intentionally NOT implemented in the template. Enterprises differ widely in their bespoke auth schemes, so these MUST be added to suit the specific production environment in which an application built from this template is deployed.
3. Credentials are held as `pydantic.SecretStr` so they do not surface in logs, reprs, or settings dumps.

Observability (correlation)
1. Every request carries a correlation ID (inbound `X-Request-ID` honoured, else generated) propagated via a ContextVar and stamped on all logs; streaming ingest assigns a per-batch ID. Boundary timings are logged to locate bottlenecks, complementing the Prometheus latency histograms.
```

- [ ] **Step 5: Verify the suite still passes (docs/comments only)**

Run: `pytest -q`
Expected: PASS — this task changed only docs/comments/docstrings.

- [ ] **Step 6: Commit**

```bash
git add persistence/stream_store/lsm_store.py services/stream_ingest.py routers/data.py CLAUDE.md ../CLAUDE.md
git commit -m "docs: single-writer LSM contract, inbound-size resilience policy, security posture"
```

---

## Final verification

- [ ] **Run the entire test suite**

Run: `pytest -q`
Expected: PASS — all existing and new tests green.

- [ ] **Confirm the module-level app still imports (no missing defaults)**

Run: `python -c "import main; print(type(main.app).__name__)"`
Expected: prints `FastAPI`.

---

## Self-Review

**Testing-philosophy compliance (the reason for this revision):** every new test drives a real HTTP endpoint, a real CORS preflight, the real `/metrics` scrape, or the real `create_app` lifespan against real testcontainers. The only non-endpoint assertions are: Task 1's `isinstance` (a config-contract invariant a `SecretStr→str` revert can't be caught any other way) and Task 3's `_run_row_count` (tombstone memory reclamation has no HTTP-visible signal). No `monkeypatch`, no mocks, no fakes remain. `caplog`/`cid_caplog` observe real logs from real code under real requests.

**Spec coverage:**

| # | Finding | Task |
|---|---------|------|
| 1 | LSM single-writer is illustrative — document it | Task 12 |
| 2 | ClickHouse retry/backoff like Redis/Postgres | Task 2 |
| 3 | Max inbound body size (HTTP 413 + streaming bound/log) + root CLAUDE.md | Tasks 5, 6, 12 |
| 4 | Reclaim LSM tombstones | Task 3 |
| 5 | Default Redis can't run the JSON code | Task 4 |
| 6 | CORS + document authn/authz/headers out of scope | Tasks 7, 12 |
| 7 | End-to-end correlation IDs (+ per-batch ingest, timing) | Tasks 8, 9 |
| 8 | `pydantic.SecretStr` | Task 1 |
| 9 | Health/readiness error-string disclosure | Task 10 |
| 10 | Metric conflates "never" with "just now" | Task 11 |

All ten findings map to at least one task. Type/name consistency checked across tasks: `correlation_id_var`, `get_correlation_id`, `set_correlation_id`, `new_id`, `CorrelationIdFilter`, `CorrelationIdMiddleware`, `timed`, `cid_caplog`, `MaxBodySizeMiddleware`, `max_request_body_bytes`, `max_ingest_batch_bytes`, `connect_max_attempts/base_delay/max_delay`, `cors_allow_*`, `correlation_id_header`. No placeholders; every code step shows complete code.
```