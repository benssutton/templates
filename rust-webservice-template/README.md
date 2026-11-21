# rust-webservice-template

An idiomatic axum/Tower service (Rust 2021) with the same HTTP contract as
its Python and Java siblings: same endpoints, JSON field names, error bodies
and status codes, so a client or a contract test cannot tell them apart.
Inside, it is written the way an axum service is written: one Tower
`ServiceBuilder`-style layer stack, typed extractors, `AppError` as the one
error type, and traits only at real seams.

Rust is the repo's choice for CPU-heavy or highly concurrent work (root
README, "Languages"). Here that is the LSM stream store: merge and compaction
run on arrow-rs compute kernels on tokio's blocking pool, and readers never
block the single writer.

## Stack
- **axum 0.8** on **tokio**; HTTPS through **axum-server + rustls** (ring
  provider, never aws-lc); **tower / tower-http** for request ids, tracing,
  timeouts, load shedding, compression, CORS, body limits and path
  normalisation
- **sqlx** (Postgres, versioned migrations), **clickhouse** (HTTP/RowBinary,
  versioned migrations applied by the app), **redis** `ConnectionManager` +
  RedisJSON
- **arrow-rs** + **arc-swap** for the LSM stream store
- **config + dotenvy + secrecy + garde** for typed, validated settings
- **tracing** (JSON logs) + optional **OpenTelemetry** OTLP export,
  **metrics + metrics-exporter-prometheus**, **sysinfo**
- **utoipa + utoipa-axum** (OpenAPI generated from the routes) + Swagger UI;
  **rmcp** (streamable-HTTP MCP server); **jsonwebtoken** (HS256 auth seam)
- Optional, behind the `solace` Cargo feature: **fe2o3-amqp** (Solace over
  AMQP 1.0) and **reqwest** SEMP v2 provisioning
- **Testcontainers + reqwest** integration tests against real Postgres,
  ClickHouse, Redis Stack and Solace (no mocks); **criterion** benchmark;
  **k6** (the shared scripts); **cargo-llvm-cov** gate (≥ 90 % lines);
  **cargo-deny**

## The shared contract
What every sibling implements, and the integration tests here pin:

| Rule | Where |
|---|---|
| snake_case JSON; root field `"MCP"` | [src/schemas/](src/schemas/) |
| every error is `application/json` `{"detail": ...}`: 400 unreadable body, 401, 404, 405, 413, 422 invalid, 500, 503 | [src/error.rs](src/error.rs), [src/extract.rs](src/extract.rs), [src/core/json_errors.rs](src/core/json_errors.rs) |
| a store outage on a request path is 503 `{"detail": "Service Unavailable"}` within a bounded time | `From<sqlx/redis/clickhouse error> for AppError`; store timeouts in settings |
| `GET /data` ordered by `id` | [src/services/data.rs](src/services/data.rs) |
| `POST /data/ingest`: every batch validated before any is written; a decoded batch over the limit is 413 | [src/services/stream_ingest.rs](src/services/stream_ingest.rs) |
| `X-Request-ID` adopted only if 1-128 chars of `[A-Za-z0-9._-]` | [src/core/request_id.rs](src/core/request_id.rs) |
| auth seam (`AUTH_ENABLED`, HS256), off by default | [src/core/auth.rs](src/core/auth.rs) |
| `INGEST_TRANSPORT` defaults to `noop`; containers listen on 8443 | [src/settings.rs](src/settings.rs), [Dockerfile](Dockerfile) |
| `MCP_ALLOWED_HOSTS` as JSON array or comma-separated | [src/settings.rs](src/settings.rs) |

## Feature map (entry points)

| Feature | Entry point |
|---|---|
| Bootstrap, routes and the layer stack | [src/app.rs](src/app.rs), [src/main.rs](src/main.rs) |
| Settings (one validation, garde rules on the fields) | [src/settings.rs](src/settings.rs) + [.env.example](.env.example) |
| HTTPS / plain HTTP, graceful drain, container healthcheck | [src/server.rs](src/server.rs) |
| Shared state (no DI container) | [src/state.rs](src/state.rs) |
| Errors and extractors | [src/error.rs](src/error.rs), [src/extract.rs](src/extract.rs) |
| `/config` (Postgres) | [src/routers/config.rs](src/routers/config.rs), [src/services/config.rs](src/services/config.rs), [src/persistence/transaction_store/postgres.rs](src/persistence/transaction_store/postgres.rs) |
| `/data` (ClickHouse) | [src/routers/data.rs](src/routers/data.rs), [src/services/data.rs](src/services/data.rs), [src/persistence/analytics_store/clickhouse.rs](src/persistence/analytics_store/clickhouse.rs) |
| `/data/cache` + `/data/ingest` (LSM) | [src/routers/data.rs](src/routers/data.rs), [src/services/stream_ingest.rs](src/services/stream_ingest.rs), [src/persistence/stream_store/lsm/mod.rs](src/persistence/stream_store/lsm/mod.rs) |
| `/cache` (Redis + RedisJSON) | [src/routers/cache.rs](src/routers/cache.rs), [src/services/cache.rs](src/services/cache.rs) |
| `/health/{live,ready,status}` (cached probes) | [src/routers/health.rs](src/routers/health.rs), [src/services/health.rs](src/services/health.rs), [src/health/](src/health/) |
| `/metrics` (Prometheus) | [src/routers/metrics.rs](src/routers/metrics.rs), [src/services/metrics.rs](src/services/metrics.rs) |
| `/mcp` (tools mirroring the REST reads; Host/Origin allow-list) | [src/mcp/](src/mcp/) |
| `/docs`, `/api-doc/openapi.json` | [src/openapi.rs](src/openapi.rs) |
| Auth seam | [src/core/auth.rs](src/core/auth.rs) |
| Logs and traces | [src/core/telemetry.rs](src/core/telemetry.rs) |
| Server-Timing attribution | [src/core/boundary_timing.rs](src/core/boundary_timing.rs) |
| Fail-fast connect with backoff | [src/core/retry.rs](src/core/retry.rs), [src/persistence/mod.rs](src/persistence/mod.rs) |
| Migrations | [migrations/postgres/](migrations/postgres/), [migrations/clickhouse/](migrations/clickhouse/) |
| Stream ingestion (settle after write, backoff, watchdog) | [src/services/stream_ingest.rs](src/services/stream_ingest.rs), [src/ingestion/](src/ingestion/) |
| Solace consumer + SEMP provisioning (`solace` feature) | [src/ingestion/solace/](src/ingestion/solace/) |
| Compose demo publisher (`solace` feature) | [src/bin/demo_publisher.rs](src/bin/demo_publisher.rs) |
| LSM benchmark | [benches/lsm.rs](benches/lsm.rs) |
| Shared Testcontainers + Solace leases | [tests/it/support/containers.rs](tests/it/support/containers.rs) |

## The request path
Outermost first (`src/app.rs`): drop an unacceptable inbound request id →
set/propagate `X-Request-ID` → `TraceLayer` (request span with the id and
any W3C `traceparent` parent; one access-log line per request with status
and latency) → gzip compression → JSON error bodies for responses made
outside a handler → load shedding over `MAX_CONCURRENT_REQUESTS` (503) →
`REQUEST_TIMEOUT_SECONDS` (503) → CORS → body limit (413) → Server-Timing →
last-request tracking → auth (when on) → Prometheus metrics (per matched
route) → handler. `POST /data/ingest` has its own in-flight bound
(`INGEST_MAX_IN_FLIGHT`, 503), so decoded bodies cannot pile up behind the
single LSM writer. Trailing slashes are normalised before routing
(`/config/` is `/config`).

## Ingest and the LSM store
Both ingest paths go through `StreamIngestService::ingest`: every batch of a
message is size-checked (`MAX_INGEST_BATCH_BYTES`, measured like pyarrow's
`nbytes`), validated by the store (key and `op` columns, a schema consistent
with the first batch) and against the read model `GET /data/cache` serves
(`id` a non-negative integer, `name` and `value` non-null strings), all
before any batch is written. Over HTTP that is 413 or 422 with nothing
written; on the stream, a reject. Arrow IPC is decoded on the blocking pool.

The store holds at most `LSM_MAX_ROWS` rows: over it, it first compacts
(dropping shadowed versions and tombstones, which loses nothing) and only
then evicts the oldest rows, counted in `lsm_evicted_rows_total` and
`/health/status` (`ingest.store`). A read builds the merged view of the
current snapshot once and caches it on the snapshot, so repeated reads cost
a slice.

## Choosing a broker (the `solace` feature)
The default build has no broker: `INGEST_TRANSPORT=noop`, and
`POST /data/ingest` feeds the store. Build with `--features solace` for
Solace Guaranteed messaging over AMQP 1.0. Facts about Solace over AMQP that
the code depends on (checked against `solace/solace-pubsub-standard:10.26.10`;
re-check them against a live broker after a broker upgrade):

| Question | Answer | Lives in |
|---|---|---|
| How is the queue created? | SEMP v2 at startup, before the first attach (an attach to a missing queue creates it without the topic subscription) | `src/ingestion/solace/provision.rs` |
| Does AMQP need enabling on the VPN? | No, the default VPN listens on 5672 | compose `solace-init` `ENABLE_AMQP: "false"` |
| Topic address form | `topic://<name>` (publish only) | `src/ingestion/solace/amqp.rs` |
| SMF binary payload arrives as | one `data` section (several are concatenated; `amqp-value(binary)` also accepted) | `amqp::payload_bytes` |
| Redelivered? | `header.delivery_count > 0` (`first_acquirer` is always false on Solace) | `amqp::redelivered` |
| Paused broker detection | the 5 s AMQP idle timeout; without it `recv` and attach block forever | `amqp::IDLE_TIMEOUT_MS` |
| Second receiver on an exclusive queue | attaches as a standby without error, so `Connected` means "transport up"; a standby counts for `bound_epoch` | `src/ingestion/solace/mod.rs` |
| fe2o3-amqp behaviour | PLAIN is refused client-side when the VPN offers only ANONYMOUS; `OpenError`'s Display hides the reason (log `{e:?}`); always close the old connection, or it keeps the exclusive queue | `amqp.rs`, `solace/mod.rs` |
| SASL | empty `SOLACE_USERNAME` = ANONYMOUS, otherwise PLAIN | `amqp::sasl_profile` |

Consumer errors: `INGEST_MAX_CONSECUTIVE_FAILURES` (default 5) consecutive
`next_delivery` failures, or the transport out of CONNECTED for
`INGEST_MAX_DISCONNECT_SECONDS`, trigger a graceful shutdown (the orchestrator
restarts the process). The two triggers are independent: empty
`INGEST_MAX_DISCONNECT_SECONDS` disables the watchdog, `0` disables the
failure count. Transport retries wait `INGEST_RETRY_BASE_DELAY` seconds
(default 1), doubling up to `INGEST_RETRY_MAX_DELAY` (default 60, not below the
base).

Ingest metrics: `ingest_outcomes_total{outcome="accepted"|"failed"|"rejected"}`
counts transport deliveries and `POST /data/ingest` requests (all three series
exist at 0 from startup); `ingest_rows_ingested_total` counts rows written.
Process gauges are named `app_process_*` (`cpu_percent`, `memory_rss_bytes`,
`num_threads`, `open_files`, `uptime_seconds`).

## Build & test
Prerequisites:
- A Rust toolchain at or above `rust-version` in [Cargo.toml](Cargo.toml).
- `cargo install cargo-llvm-cov cargo-deny` and
  `rustup component add llvm-tools-preview`.
- Docker Desktop or Docker Engine with ≥ 3 GiB of memory.
- Python for `run_CICD.py` (the repo's `p315` conda env, see the root
  `CLAUDE.md`; CI uses 3.13), with `solace-pubsubplus` and `pyarrow` for the
  SMF interop test.

```bash
cd rust-webservice-template
cargo test --all-targets                      # default build (no Solace)
SMF_PYTHON=/path/to/python cargo test --all-features --all-targets
cargo bench --bench lsm
```
From the repo root, `python run_CICD.py rust ci` runs fmt-check, clippy
(both feature sets, `-D warnings`), one instrumented test run with every
feature on, the coverage report (≥ 90 % lines), the image build, Trivy and
k6 smoke; `python run_CICD.py rust deny` runs cargo-deny.

## Run the full stack
```bash
docker compose up -d --build --wait
curl -fsk https://localhost/health/ready
# Swagger UI: https://localhost/docs  •  MCP: https://localhost/mcp
```
The app listens on 8443 in its container; compose publishes it on host port
443. Compose runs the app (with `INGEST_TRANSPORT=solace`), Postgres
(pgvector), ClickHouse (seeded), Redis Stack, the Solace broker, a one-shot
`solace-init` (SEMP provisioning), a demo publisher and `dev-certs`, which
writes a self-signed certificate into a named volume the app mounts: the
image carries no key. Without a broker:
```bash
docker compose up -d db clickhouse redis dev-certs
INGEST_TRANSPORT=noop docker compose up -d --no-deps app
```

On the host, plain HTTP:
`SSL_CERTFILE= SSL_KEYFILE= cargo run --bin rust-template`, with `.env`
pointing at reachable stores (compose does not publish the store ports). For
HTTPS on the host, mint a certificate with
`bash certs/generate_self_signed_cert.sh` (needs `openssl`; Git Bash ships
it). It is for development only: CN `localhost`, SANs `localhost`, `app` and
`127.0.0.1`. Clients must skip verification (`curl -k`).

## Adding things
- **An endpoint.** Write the handler in `src/routers/<area>.rs` with a
  `#[utoipa::path(...)]` that lists its error responses
  (`body = ErrorResponse`), take input through `ValidJson<T>` /
  `ValidQuery<T>` (put garde rules on `T` in `src/schemas/`), return
  `Result<_, AppError>`, and add `.routes(routes!(handler))` in
  `app::router`. The route, the OpenAPI entry and the layer stack come with
  it. Add a test in `tests/it/` through `TestApp`.
- **A store.** Add a module under `src/persistence/`, connect it through
  `connect_with_backoff` with a smoke test, implement `HealthProbe`, put it
  in `Stores` (and `Stores::probes`), give its client timeouts from
  settings, and add `impl From<its error> for AppError` mapping outages to
  `Unavailable`. Schema changes are new files in `migrations/`.
- **An MCP tool.** Add a `#[tool]` method in `src/mcp/tools.rs` that calls
  the same service as the REST endpoint it mirrors, and return
  `rest_body(&...)` (one text item holding the REST body); extend
  `the_tools_mirror_the_rest_read_endpoints`.

## Performance profiling
See [PERF_PROFILING_RUNBOOK.md](PERF_PROFILING_RUNBOOK.md): Layer 1 (k6 +
Server-Timing) and Layer 2 (perf + flamegraph).
