# Rust Template

An idiomatic axum/Tower service with a fixed HTTP contract. Read
[README.md](README.md) first: the shared contract, the
feature map, the request path and the "Adding things" recipes.

## Commands
From the repo root (the `p315` env, see the root `CLAUDE.md`):
`python run_CICD.py rust fast` (fmt-check + clippy on both feature sets),
`python run_CICD.py rust test` (one instrumented run, all features),
`python run_CICD.py rust deny`. In this directory:
`cargo test --test it <module>::` (one integration module),
`cargo test --lib <path>` (unit tests), add `--features solace` for the
Solace modules. Never point tests at a developer's running compose stack.

## Layout
```
src/main.rs        settings -> telemetry -> bind -> create_app -> signals -> serve; `healthcheck` subcommand
src/app.rs         create_app; router(): routes (utoipa-axum) + the layer stack, outermost last
src/extract.rs     ValidJson / ValidQuery (garde) / ArrowStream: rejections are AppError
src/error.rs       AppError -> {"detail"}; From<store errors> splits outage (503) from bug (500)
src/core/          request_id, json_errors, auth, telemetry, boundary_timing, cors, retry, shutdown, ...
src/services/      config, data, cache, health (cached probes), metrics, stream_ingest
src/persistence/   stores (connect + smoke test + probe + migrate); stream_store/lsm/ (mod, kernels, tests)
src/ingestion/     BatchConsumer / Settle seam, noop, ipc; solace/ behind the `solace` feature
src/mcp/           rmcp service + tools mirroring the REST reads
migrations/        postgres/ (sqlx, embedded) and clickhouse/ (applied by the app)
tests/it/          THE integration-test binary (main.rs + one module per feature + support/)
```

## Key patterns
- **Test isolation.** Every test builds its own app (`TestApp::spawn`) on an
  ephemeral port and calls it over real HTTP. No `oneshot`, no mocks;
  in-process tests implement a seam for real (`HealthProbe`,
  `IngestHealthProvider`, `BatchConsumer`, `Settle`, `StreamStore`).
- **One integration binary, shared containers** (`tokio::sync::OnceCell`
  statics in `support/containers.rs`). Tests that stop, pause or break a
  store use a `Dedicated` container. Use per-test `uuid` keys; Solace tests
  use `SolaceNames::new()`.
- **Errors**: handlers return `AppError`; extractors produce the 400/413/422s.
  `unwrap`/`expect` are denied by `[lints]` outside test code.
- **Fail fast, shut down cleanly.** Stores and the consumer connect through
  `connect_with_backoff` plus a smoke test; `create_app` closes what already
  connected on failure. Shutdown: stop the consumer -> drain HTTP (MCP event
  streams end with it) -> close consumer and stores (`App::close`).
- **Settle after write; CPU off the async workers.** Arrow decoding, LSM
  merge/compaction and sysinfo run on the blocking pool; each LSM write is a
  detached task, so a dropped request still records its rows.
- **Helpers added before their first user** get
  `#[expect(dead_code, reason = "...")]`; remove it with the first use.

## Environment gotchas
- **Docker Desktop with ≥ 3 GiB VM memory**; the Solace broker alone needs
  about 1 GiB and is polled for readiness for up to 180 s.
- **Windows Docker endpoint:** testcontainers' default npipe works; if Docker
  is unreachable, compare `docker context ls` and export `DOCKER_HOST`.
- **Address test containers as `127.0.0.1`, never `localhost`** (IPv6 `::1`
  first costs ~200 ms per connection on Windows). MCP tests send an explicit
  `Host: localhost:<port>`. A refused loopback connect takes ~2 s on Windows.
- **Every container goes through `containers::start()`** (labels, stale
  sweep, 127.0.0.1, retry of the mapped-port lookup) and **every Docker call
  runs on `containers::on_docker`** (one runtime owns the cached client).
- **Container cleanup:** a `dtor` hook removes shared containers; after a
  failed run on Windows the next run sweeps them by PID label. Manual:
  `docker rm -f $(docker ps -aq --filter label=rust-template-it=true)`.
- **`test_settings()`** wires no stores; **`app_settings().await`** points at
  the shared containers.
- **Never name a test module after a crate** (`clickhouse`, `redis`,
  `arrow`): it shadows the crate. Write `::clickhouse`, `::redis`.
- **Solace leases:** `containers::solace()` is a read lease,
  `solace_exclusive()` (pause/unpause) the write lease. Never take both in
  one test: it deadlocks.
- **SEMP counters lag ~5 s:** poll (`wait_for_depth`, `wait_until`), never sleep.
- **Log capture:** `support::logs::capture()`; never install another global
  subscriber. For `spawn_blocking` or threads, carry the dispatcher.
- **`SMF_PYTHON`:** without it `smf_interop` is skipped locally (fails under
  `CI=true`); the pipeline sets it.
- **Paused-clock tests** need tokio `test-util`; no real I/O in them.
- **TLS is ring, never aws-lc.** New crates go in with
  `default-features = false`; `cargo tree -i aws-lc-sys -e normal,dev` must
  match nothing, and deny.toml bans it.
- **Line endings:** `.gitattributes` forces LF for `*.sh`, `*.sql`, the
  Dockerfile, compose files and `.env.example` (sqlx checksums migrations,
  and CRLF scripts fail in Linux containers).
- **k6 scripts are shared byte for byte** (root
  `tests/cicd/test_perf_scripts.py`): change every service's copy together.
  Under Git Bash prefix `docker run ... /scripts/...` with `MSYS_NO_PATHCONV=1`.
- **Coverage:** `--fail-under-lines 90`, no exclusions; a file that drags it
  down needs a test.
- **Windows system metrics:** `num_threads` and `open_files` are 0 off Linux.
- **Test credentials** carry `// gitleaks:allow (test-only ...)`; never weaken
  the root `.gitleaks.toml`.
