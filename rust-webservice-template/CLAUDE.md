# Rust Template

An axum (Rust) webservice that mirrors `python-webservice-template` feature for
feature (same endpoints, shapes, setting names and resilience behaviour), with
the LSM stream store on arrow-rs compute kernels. Read [README.md](README.md)
first: it has the feature map and the list of deliberate divergences.

## Architecture
```
src/main.rs            thin bootstrap: tracing → Settings::from_env → bind → create_app → signals → server::serve
src/app.rs             create_app(settings) -> App { router, state }; router(): the middleware order; App::close
src/server.rs          bind / serve (plain HTTP or rustls HTTPS), graceful drain, install_crypto_provider (ring)
src/settings.rs        Settings (Python's names, envy + dotenvy, SecretString secrets), validate()
src/state.rs           AppState: Arc'd settings, stores, concrete services, ingest service, Shutdown
src/error.rs           AppError → 400/404/413/422/503/500, body {"detail": ...}
src/core/              correlation, boundary_timing (Server-Timing), cors, request_limits, last_request,
                       retry (connect_with_backoff), shutdown (CancellationToken), system_metrics
src/health/            HealthProbe trait + ProbeRegistry (concurrent, per-probe timeout); IngestHealthProvider
src/persistence/       Stores (concurrent connect + schema + probes)
  transaction_store/postgres.rs   sqlx PgPool (pgvector image)
  analytics_store/clickhouse.rs   clickhouse::Client
  cache_store/redis.rs            ConnectionManager + RedisJSON
  stream_store/        StreamStore trait (ingest, validate, query); lsm.rs: single-writer LSM (tokio Mutex writer, ArcSwap snapshot)
src/ingestion/         ConnectionState, Delivery, Settle, BatchConsumer; ipc.rs (codec, batch_bytes); noop.rs
  solace/              SolaceBatchConsumer (AMQP 1.0), amqp.rs, publisher.rs, provision.rs, semp.rs (SEMP v2)
src/services/          health, metrics, config, data, cache, stream_ingest (settle-after-write task)
src/routers/  src/schemas/  src/openapi.rs  src/mcp/ (get_health_status; Host allow-list)
src/bin/demo_publisher.rs      compose demo publisher (thin wrapper over AmqpPublisher)
scripts/               postgres-init.sql, clickhouse-init.sql, clickhouse-seed.sh, solace-init.sh
benches/lsm.rs         criterion: LSM merge + full compaction
tests/it/              THE integration-test binary: main.rs + one module per feature + support/
  support/             TestApp, test_settings/app_settings, containers, semp, solace, batches, gated_store, logs
tests/test_data/       clickhouse_seed_data.ipc (copied from Python)
tests/performance/     k6 scripts (Python's, byte-identical) + profile/run_perf.sh
```

## Stack
See [README.md#stack](README.md#stack).

## Key patterns
- **`create_app` isolation.** Every test builds its own app from its own
  `Settings` (`TestApp::spawn`), serves it on an ephemeral port and calls it
  over real HTTP with reqwest. No `oneshot`, no mocks.
- **One integration-test binary, shared containers.** Each `tests/*.rs` file
  would be its own binary, so everything lives in `tests/it/`. Postgres,
  ClickHouse, Redis Stack and Solace start once per run, behind
  `tokio::sync::OnceCell` statics in `tests/it/support/containers.rs`. Tests
  that stop or break a store use a `Dedicated` container of their own, never a
  shared one. Writes use per-test `uuid` keys, and Solace tests use
  `SolaceNames::new()` (`test/<hex>/batches`, `test-<hex>`), because tests run
  in parallel.
- **Server-Timing scope.** `core::boundary_timing` keeps a `tokio::task_local!`
  sample buffer that the middleware scopes around each request, the
  counterpart of Python's `ContextVar`. `timed(label, fut)` records into it
  through a drop guard. The ingest task runs outside any request scope, so its
  timings never leak into a response header.
- **Traits only at real seams**: `HealthProbe`, `IngestHealthProvider`,
  `BatchConsumer`, `Settle`, `StreamStore`. Services are concrete structs in
  `Arc`. In-process tests implement a seam for real (a scripted consumer, a
  store whose write fails, `support::gated_store::GatedStore`); they never patch.
- **Fail fast, then shut down cleanly.** Every store and the consumer (after
  SEMP provisioning) connect through `connect_with_backoff`, followed by a
  smoke test; `create_app` returns `Err`, closing whatever already connected,
  and `main` exits non-zero. SIGTERM, the ingest watchdog and five consecutive
  consumer errors all trigger the one `Shutdown`. The order is: stop the
  consumer → drain HTTP → close the consumer and the stores (`App::close`).
- **Settle after write.** `StreamIngestService` size-checks and `validate`s
  every batch of a delivery, writes them, then settles once: accept, or fail
  with jittered backoff, or reject. The consumer itself rejects undecodable
  payloads. Settle a delivery before asking for the next one.
- **LSM CPU work stays off the async workers**: `LsmStore::ingest` takes the
  writer mutex with `lock_owned().await`, then runs merge/compaction in
  `spawn_blocking`. Each write is a detached task, so a dropped HTTP request
  still records its rows.
- **Errors**: handlers return `AppError`. No `unwrap`/`expect` in `src/`
  outside `main.rs`, `src/bin/` and `#[cfg(test)]`.

## Spike decisions (AMQP 1.0 against Solace; D1–D11)
From the R1 spike against `solace/solace-pubsub-standard:10.26.10`.

| ID | Decision | Value | Lives in |
|---|---|---|---|
| D1 | Queue provisioning variant | (b) SEMP v2 at startup, before the first attach: `SELF_PROVISIONING = false` | `src/ingestion/solace/provision.rs` |
| D2 | AMQP needs a SEMP PATCH on the VPN | no: `AMQP_NEEDS_SEMP_ENABLE = false` | `tests/it/support/containers.rs`, compose `solace-init` `ENABLE_AMQP: "false"` |
| D3 | Topic address form | `TOPIC_ADDRESS_PREFIX = "topic://"` (publish only there) | `src/ingestion/solace/amqp.rs` |
| D4 | SMF binary payload arrives as | one `data` section (several are concatenated; `amqp-value(binary)` also accepted) | `amqp::payload_bytes` |
| D5 | `Delivery.redelivered` rule | `header.delivery_count > 0` (never `first_acquirer`, always false on Solace) | `amqp::redelivered` |
| D6 | Idle timeout that surfaces a paused broker | `IDLE_TIMEOUT_MS = 5_000` (mandatory: without it `recv` and attach block forever) | `src/ingestion/solace/amqp.rs` |
| D7 | Attach source | n/a under (b): the plain queue name | `provision::source` |
| D8 | SEMP redelivery counter field | `REDELIVERED_FIELD = "redeliveredMsgCount"` | `tests/it/support/semp.rs` |
| D9 | Second receiver on an exclusive queue | attaches as a standby without error, so `Connected` means "transport up", not "flow active"; a standby counts for `bound_epoch` | `src/ingestion/solace/mod.rs` |
| D10 | fe2o3-amqp API corrections | none to signatures; behaviour: PLAIN is refused client-side when the VPN offers only ANONYMOUS; `OpenError`'s Display hides the reason, so log `{e:?}`; a dead peer surfaces on `recv` as `LinkStateError(SessionStopped(ConnectionStopped(Closed)))`; always close the old connection (also when `attach_receiver` fails), or it keeps the exclusive queue | `amqp.rs`, `solace/mod.rs` |
| D11 | Reconnect after unpause | immediate (first attempt) once the 5 s idle timeout has torn the old link down | `tests/it/solace_resilience.rs` timeouts |

D2–D6 and D8 carry a `[SPIKE D<n>]` doc comment: `grep -rn "SPIKE D" src tests`.
The spike questions came from the gaps between the fe2o3-amqp docs and Solace's
AMQP behaviour; each row's Decision column is one of them, answered by a throwaway
client run against the broker container. If a broker upgrade changes the behaviour,
re-check the affected rows against a live broker before changing a value.

## Environment gotchas
- **Docker Desktop with ≥ 3 GiB VM memory.** The shared Solace broker needs
  about 1 GiB on top of the stores and is polled for readiness (up to 180 s)
  on the first test that asks for it.
- **Windows Docker endpoint:** no `DOCKER_HOST` needed; testcontainers'
  default `npipe:////./pipe/docker_engine` works. If Docker is unreachable,
  compare with `docker context ls` and export `DOCKER_HOST` to the active endpoint.
- **Test containers are addressed as `127.0.0.1`, never `localhost`.** On
  Windows `localhost` tries IPv6 `::1` first, costing about 200 ms per new
  connection, enough to push probes past their 2 s timeout. `containers::start()`
  rewrites the host; MCP tests connect to 127.0.0.1 and send an explicit
  `Host: localhost:<port>` (the default `mcp_allowed_hosts` is `["localhost"]`).
  A refused loopback connect also takes about 2 s on Windows: bound
  closed-port timings accordingly.
- **Every container goes through `containers::start()`**, which adds the
  labels, the stale-container sweep, the 127.0.0.1 host and a 5 s retry of the
  mapped-port lookup (Docker Desktop can briefly report `PortNotExposed`
  right after start).
- **Every Docker call in the tests runs on one dedicated runtime**
  (`support::containers::on_docker`). testcontainers caches one Docker client
  per process, and `#[tokio::test]` makes a runtime per test; without
  `on_docker`, later tests would reuse connections owned by a finished
  runtime. Use `on_docker` for `docker pause`/`unpause` too.
- **Container cleanup.** Statics are never dropped and there is no Ryuk
  reaper, so a `dtor` hook removes the shared containers at exit. On Windows,
  after a failed test, libtest's `process::exit` skips that hook, so every
  container also carries a `rust-template-it.pid=<pid>` label and the next run
  sweeps containers whose process is gone (`sweep_stale_containers`). Manual
  cleanup: `docker rm -f $(docker ps -aq --filter label=rust-template-it=true)`.
- **`test_settings()` vs `app_settings().await`:** `test_settings()` wires no
  stores, for tests that never connect, such as invalid-settings startup.
  `app_settings().await` points at the shared containers. Any test that builds
  a working app needs the second.
- **Never name a test module after a crate** (`clickhouse`, `redis`, `arrow`,
  `fe2o3_amqp`). A `mod clickhouse;` at the test-crate root shadows the crate
  for every module, which is why the store tests are `store_postgres`,
  `store_clickhouse` and `store_redis`. Write `::clickhouse` and `::redis` for
  the crates where a name could clash.
- **Solace broker leases:** `containers::solace()` takes a read lease, and
  `solace_exclusive()` (the only way to `pause`/`unpause`) takes the write
  lease, so a paused broker never breaks a concurrent test. **Never take two
  leases in one test**: a read lease held while waiting for the write lease
  deadlocks the run.
- **SEMP counters lag.** Queue depth and similar SEMP stats trail an accept by
  about 5 s: poll (`support::solace::wait_for_depth`, `wait_until`) with a
  timeout of 10 s or more; never sleep a fixed time.
- **Log capture:** `support::logs::capture()` calls `ensure_installed()` (the
  one global TRACE subscriber) and then a thread-local `set_default`. Never
  install another global subscriber. It only sees logs from tasks
  `tokio::spawn`ed under a plain `#[tokio::test]`; for `spawn_blocking`,
  threads or a multi-thread runtime, carry the dispatcher
  (`dispatcher::get_default(|d| d.clone())` + `with_default`).
- **`SMF_PYTHON`** (setup in [README.md#build--test](README.md#build--test)):
  a local `cargo test` without it silently skips `smf_interop`, so a green run
  proves nothing about SMF interop. Under `CI=true` the missing variable fails.
- **Paused-clock tests** (`#[tokio::test(start_paused = true)]`, in
  `tests/it/stream_ingest.rs`) need tokio's `test-util` dev feature. Time
  auto-advances whenever every task is idle, so never do real network I/O in them.
- **Helpers added before their first user** get
  `#[expect(dead_code, reason = "...")]`; delete it in the change that first
  uses the helper, or clippy `-D warnings` fails on the unfulfilled expectation.
- **TLS crypto provider is ring, never aws-lc.** New crates go in with
  `default-features = false` plus only what they need, and
  `cargo tree -i aws-lc-sys -e normal,dev` must print "did not match any
  packages". aws-lc-sys needs extra native tooling on Windows MSVC.
- **Toolchain:** `rust-version = 1.94` (sqlx 0.9 requires it); developed on 1.96.
- **Line endings:** `.gitattributes` forces LF for `*.sh`, `*.sql`, the
  Dockerfile, compose files and `.env.example`, and marks `*.ipc` binary. The
  checkout uses `core.autocrlf=true`, and a CRLF shell script fails inside
  Linux containers.
- **k6 under Git Bash:** prefix `docker run ... /scripts/<file>.js` with
  `MSYS_NO_PATHCONV=1`, or Git Bash rewrites `/scripts/...` into a Windows
  path. `run_perf.sh` sets it itself.
- **Coverage (cargo-llvm-cov):** `--fail-under-lines 90` with no exclusions
  (`main.rs` and `src/bin/` count too). Any file that drags the total
  under 90 % needs a test, not an exclusion.
- **Windows system metrics:** `num_threads` and `open_files` report 0 off
  Linux (sysinfo limits).
- **Test credentials** (`PG_PASSWORD`, `CH_PASSWORD` in `containers.rs`) carry
  `// gitleaks:allow (test-only container credential)`. Never weaken the root
  `.gitleaks.toml`.
- **k6 scripts are Python's, byte for byte** (root `tests/cicd/test_perf_scripts.py`):
  change them in Python and copy, never edit the Rust copy alone.

## Running checks
From the repo root: `python run_CICD.py rust fast` (fmt-check + clippy),
`python run_CICD.py rust ci` (everything GitHub runs). In this directory:
`cargo test --test it <module>::` for one integration module,
`cargo test --lib <path>` for unit tests inside `src/`.

## Database investigation
Run the relevant module; each run starts its own containers:
`cargo test --test it store_postgres::` (Postgres), `store_clickhouse::`
(ClickHouse), `store_redis::` (Redis), `solace_broker::` (Solace + SEMP).
Never point tests at a developer's running compose stack.
