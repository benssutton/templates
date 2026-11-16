# rust-webservice-template

An axum (Rust 2021) service that mirrors `python-webservice-template`
feature for feature, so a developer can read either codebase and map one onto
the other. It has the same endpoints, request/response shapes, setting names
and resilience behaviour, with minimal business logic: a reusable foundation
wired to common technologies.

Rust is the repo's choice for CPU-heavy or highly concurrent work (root
README, "Languages"). Here that is the LSM stream store: merge and compaction
run on arrow-rs compute kernels (`lexsort_to_indices`, `filter`, `concat`),
on tokio's blocking pool, and readers never block the single writer.

## Stack
- **axum 0.8** on **tokio**; HTTPS on 443 through **axum-server + rustls**
  (ring provider); `tower-http` for CORS and the body limit
- **sqlx** (Postgres; pgvector/pgvector image, `vector` extension enabled at
  startup), **clickhouse** (official HTTP/RowBinary client), **redis**
  `ConnectionManager` + RedisJSON
- **arrow-rs** (IPC codec + compute kernels) and **arc-swap** for the LSM
  stream store; **fe2o3-amqp** (AMQP 1.0) for Solace Guaranteed messaging,
  **reqwest** for SEMP v2 provisioning
- **envy + dotenvy + secrecy** for typed settings with redacted secrets
- **tracing**, **metrics + metrics-exporter-prometheus**, **sysinfo**;
  **utoipa** + Swagger UI; **rmcp** (streamable HTTP MCP server)
- **Testcontainers + reqwest** integration tests against real Postgres,
  ClickHouse, Redis Stack and Solace (no mocks); **criterion** benchmark;
  **k6** (Python's scripts, unchanged); **cargo-llvm-cov** coverage gate (≥ 90 % lines)

LSM benchmark (`cargo bench --bench lsm`: 4 runs × 10,000 rows, half the keys
rewritten per run, one row in ten a tombstone): merge 7.88 ms, full
compaction 5.57 ms (criterion median, developer laptop).

## Feature map (entry points)

| Feature | Entry point |
|---|---|
| Bootstrap (`create_app` isolation, shutdown order) | [src/app.rs](src/app.rs), [src/main.rs](src/main.rs) |
| Typed settings (Python's names, `validate()`) | [src/settings.rs](src/settings.rs) + [.env.example](.env.example) |
| HTTPS / plain-HTTP serving, graceful drain | [src/server.rs](src/server.rs) |
| Shared state (no DI container) | [src/state.rs](src/state.rs) |
| Error → status mapping (`{"detail": ...}`) | [src/error.rs](src/error.rs) |
| `/config` (Postgres, sqlx) | [src/routers/config.rs](src/routers/config.rs), [src/services/config.rs](src/services/config.rs), [src/persistence/transaction_store/postgres.rs](src/persistence/transaction_store/postgres.rs) |
| `/data` (ClickHouse) | [src/routers/data.rs](src/routers/data.rs), [src/services/data.rs](src/services/data.rs), [src/persistence/analytics_store/clickhouse.rs](src/persistence/analytics_store/clickhouse.rs) |
| `/data/cache` + `/data/ingest` (LSM) | [src/routers/data.rs](src/routers/data.rs), [src/persistence/stream_store/lsm.rs](src/persistence/stream_store/lsm.rs) |
| `/cache` (Redis + RedisJSON) | [src/routers/cache.rs](src/routers/cache.rs), [src/services/cache.rs](src/services/cache.rs), [src/persistence/cache_store/redis.rs](src/persistence/cache_store/redis.rs) |
| `/health/{live,ready,status}` | [src/routers/health.rs](src/routers/health.rs), [src/services/health.rs](src/services/health.rs), [src/health/](src/health/) |
| `/metrics` (Prometheus) | [src/routers/metrics.rs](src/routers/metrics.rs), [src/services/metrics.rs](src/services/metrics.rs) |
| `/mcp` (`get_health_status`, Host allow-list) | [src/mcp/mod.rs](src/mcp/mod.rs), [src/mcp/tools.rs](src/mcp/tools.rs) |
| `/docs` (Swagger UI) | [src/openapi.rs](src/openapi.rs) |
| Correlation ID | [src/core/correlation.rs](src/core/correlation.rs) |
| Server-Timing attribution (`task_local!`) | [src/core/boundary_timing.rs](src/core/boundary_timing.rs) |
| CORS, body limit | [src/core/cors.rs](src/core/cors.rs), [src/core/request_limits.rs](src/core/request_limits.rs) |
| Fail-fast connect with backoff | [src/core/retry.rs](src/core/retry.rs), [src/persistence/mod.rs](src/persistence/mod.rs) |
| Graceful shutdown handle | [src/core/shutdown.rs](src/core/shutdown.rs) |
| Stream ingestion (settle after write, backoff, watchdog) | [src/services/stream_ingest.rs](src/services/stream_ingest.rs) |
| `BatchConsumer` / `Settle` contract; noop transport; Arrow IPC codec | [src/ingestion/mod.rs](src/ingestion/mod.rs), [src/ingestion/noop.rs](src/ingestion/noop.rs), [src/ingestion/ipc.rs](src/ingestion/ipc.rs) |
| Solace consumer (AMQP 1.0) + SEMP provisioning | [src/ingestion/solace/mod.rs](src/ingestion/solace/mod.rs), [src/ingestion/solace/amqp.rs](src/ingestion/solace/amqp.rs), [src/ingestion/solace/provision.rs](src/ingestion/solace/provision.rs), [src/ingestion/solace/semp.rs](src/ingestion/solace/semp.rs) |
| Compose demo publisher | [src/bin/demo_publisher.rs](src/bin/demo_publisher.rs), [src/ingestion/solace/publisher.rs](src/ingestion/solace/publisher.rs) |
| SQL DDL + compose init scripts | [scripts/](scripts/) |
| LSM merge/compaction benchmark | [benches/lsm.rs](benches/lsm.rs) |
| Shared Testcontainers (one per run) + Solace leases | [tests/it/support/containers.rs](tests/it/support/containers.rs) |
| k6 performance tests | [tests/performance/](tests/performance/) |

## Build & test
Prerequisites:
- A Rust toolchain at or above `rust-version` in [Cargo.toml](Cargo.toml) (1.94).
- For coverage: `cargo install cargo-llvm-cov` and
  `rustup component add llvm-tools-preview`.
- Docker Desktop or Docker Engine with ≥ 3 GiB of memory. Testcontainers,
  the image build, Trivy and k6 all run in Docker.
- Python 3.15 for `run_CICD.py` (the repo's `p315` conda env; see the root
  `CLAUDE.md`), with `solace-pubsubplus` and `pyarrow` installed for the SMF
  interop test.

```bash
cd rust-webservice-template
# Docker Desktop must be running with >= 3 GiB of VM memory (Testcontainers
# starts Postgres, ClickHouse, Redis Stack and a Solace broker, once per run).
export SMF_PYTHON=/path/to/python   # with solace-pubsubplus + pyarrow (SMF interop test)
cargo test --all-targets
cargo bench --bench lsm
```
Without `SMF_PYTHON` the `smf_interop` test is skipped locally (it fails
under `CI=true`). The whole pipeline, from the repo root:
`python run_CICD.py rust ci` (fmt-check, clippy `-D warnings`, tests,
coverage ≥ 90 % lines, image build, Trivy, k6 smoke). It sets `SMF_PYTHON`
itself.

## Run the full stack (HTTPS on 443)
```bash
docker compose up -d --build --wait
curl -fsk https://localhost/health/ready
# Swagger UI: https://localhost/docs  •  MCP: https://localhost/mcp
```
Compose runs the app, Postgres (pgvector), ClickHouse (seeded), Redis Stack,
the Solace broker, a one-shot `solace-init` (SEMP queue provisioning) and a
demo publisher. `app` depends on a healthy `solace`, so for HTTP-only ingest
without a broker skip that dependency with `--no-deps`:
```bash
docker compose up -d db clickhouse redis
INGEST_TRANSPORT=noop docker compose up -d --no-deps app
```

To serve HTTPS on the host (`cargo run --bin rust-template`), first run
`certs/generate_self_signed_cert.sh`, which writes `certs/key.pem` and `certs/cert.pem`.
Plain HTTP on the host for local development:
`SERVER_PORT=8000 SSL_CERTFILE= SSL_KEYFILE= INGEST_TRANSPORT=noop cargo run --bin rust-template`,
with the store settings in `.env` pointing at reachable stores (compose does
not publish the store ports; see [.env.example](.env.example)).

## How this maps to python-webservice-template
`create_app(settings)` exists in both. Python's hand-rolled DI `Container`
becomes an explicit `AppState` of `Arc`'d concrete services, and traits exist
only at real seams (`HealthProbe`, `IngestHealthProvider`, `BatchConsumer`,
`Settle`, `StreamStore`), not one per service for mocking. Everything else
mirrors the Python structure and naming. Deliberate divergences:

**Solace transport**

1. **AMQP 1.0 (fe2o3-amqp) instead of SMF**: Solace has no official Rust SDK.
   `SOLACE_PORT` therefore defaults to 5672, not 55555, and the message VPN
   is selected by the listen port (`SOLACE_VPN` is used only in SEMP paths).
   Each connection gets a unique container id (`<queue>-<8 hex>`, `/` → `_`),
   because a broker may still hold the stale connection after a partition.
2. **SASL ANONYMOUS when `SOLACE_USERNAME` is empty**, PLAIN otherwise.
   `SOLACE_USERNAME` and `SOLACE_PASSWORD` default to empty, and compose and
   the tests leave them empty, because the broker's default VPN offers only
   ANONYMOUS over AMQP. Python and Java always send credentials over SMF
   (default `admin`/`admin`).
3. **The queue is provisioned through SEMP v2 at startup**, before the first
   AMQP attach (`SOLACE_SEMP_URL`, `SOLACE_SEMP_USERNAME`,
   `SOLACE_SEMP_PASSWORD`, the last a secret), because an AMQP attach cannot
   add a topic subscription to a queue. The calls are idempotent, which
   matches SMF's `CREATE_ON_START`. Compose adds a one-shot `solace-init`
   service so the queue exists before the demo publisher starts.

**Ingest and the LSM store**

4. **arrow-rs instead of polars.** The merge is `lexsort_to_indices` (key
   ascending, seqno descending), then the first row per key, then a filter
   on `op != "delete"`. Readers load an `ArcSwap<Snapshot>`; the writer holds
   a tokio mutex and runs the CPU work in `spawn_blocking`.
5. **An async `BatchConsumer` instead of an ingest thread.** The ingest loop is
   a tokio task, and every consumer method takes `&self`, so one
   `Arc<dyn BatchConsumer>` is shared by the ingest task, shutdown and the
   health path. Python's `on_bound` callback becomes `bound_epoch()`, a count
   of successful binds: when it rises, the loop resets its consecutive-failure
   count and backoff.
6. **`Settle` methods consume the delivery's settlement** (`accept`, `fail`,
   `reject` take `self: Box<Self>`), so the compiler guarantees at most one
   settlement per delivery.
7. **`StreamStore::validate(&[RecordBatch])` checks the whole message**
   before any write, and is stricter than Python's per-batch required-column
   check: the first batch fixes the column names and types, a later batch
   with a different schema, duplicate column names, or unmergeable key/`op`
   types is rejected (422 over HTTP; `reject` on the stream). A message is
   all-or-nothing, as in Python.
8. **`max_ingest_batch_bytes` is checked against `batch_bytes`**, the sum of
   the decoded batch's buffer lengths (within a few bytes of pyarrow's
   `nbytes`; it overcounts sliced arrays, which decoding never produces).

**HTTP surface**

9. **Metrics.** Extra counters `ingest_deliveries_total{outcome}` and
   `ingest_rows_total`. `http_request_size_bytes` and
   `http_response_size_bytes` are emulated as `_count`/`_sum` counter pairs
   (so their `# TYPE` differs), there are no `_created` series, and the
   content type is `text/plain; version=0.0.4`.
10. **Error bodies.** 422 bodies are `{"detail": "<message>"}`, not FastAPI's
    list of error objects; status codes match, and serde is stricter than
    pydantic's lax mode (`"ttl_seconds": "60"` is a 422). An undecodable
    `POST /data/ingest` body is 400 as in Python, but its `detail` is
    arrow-rs's error text, not pyarrow's. Every 413 is rewritten to Python's
    plain-text `Payload Too Large` and an unrouted method to
    `{"detail": "Method Not Allowed"}`, so those match.
11. **No trailing-slash redirect.** Both forms of Python's canonical paths are
    routed (`/config` and `/config/`, `/cache` and `/cache/`); FastAPI's 307
    for other forms (`/data/`, `/cache/{key}/`) is a 404 here.
12. **MCP (rmcp)** is mounted at both `/mcp` and `/mcp/` on one service
    (shared sessions), so metrics carry both handler labels.
    `MCP_ALLOWED_HOSTS` is Rust-only: the DNS-rebinding `Host` allow-list,
    default `localhost`, which must not be empty (rmcp would then allow every
    host); other hosts get 403. Compose sets `localhost,127.0.0.1,app`.
13. **Correlation IDs** come from a small axum middleware (32-hex IDs and a
    configurable header, like Python), not tower-http's request-id layer. An
    inbound ID that is not visible ASCII is replaced, where Python echoes it.

**Settings, tooling and tests**

14. **Settings.** List settings accept Python's JSON form and comma-separated
    values. The service-identity defaults are Rust's own (`app_title`,
    `mcp_name=rust-template`, `solace_queue=rust-template/ingest`). An empty
    `SSL_CERTFILE` or `SSL_KEYFILE` serves plain HTTP.
15. **perf + inferno flamegraphs instead of py-spy** for Layer-2 profiling.
    The profiling overlay adds `SYS_PTRACE` and `SYS_ADMIN` and builds with
    frame pointers.
16. **Compose does not publish the store and broker ports** (as in Java's
    compose), so the Rust stack can run next to the Python and Java stacks.
17. **Test redesigns.** The watchdog test pauses the broker under an
    exclusive lease and trips in seconds (the AMQP idle timeout is 5 s; the
    SMF keepalive takes about 30 s). The bind-denied tests bind anonymously
    to an owner-less `no-access` queue instead of using a per-test client
    username. Python's SMF-only tests (terminal interruption, interruption
    before bind) have no Rust analogue; `smf_interop` checks that an SMF
    publisher's batches reach the AMQP consumer.
18. **Process stats on Windows.** `/health/status` reports `num_threads` and
    `open_files` as 0 (sysinfo limits). Linux and Docker report real values.
19. **Shutdown order.** Shutdown stops the ingest loop first, drains HTTP,
    then closes the AMQP link and the stores; the spec's order is consumer,
    drain, pools. The effect is equivalent: no new deliveries start, and
    unsettled deliveries are redelivered.

## Performance profiling
See [PERF_PROFILING_RUNBOOK.md](PERF_PROFILING_RUNBOOK.md): Layer 1 (k6 +
Server-Timing) and Layer 2 (perf + flamegraph).
