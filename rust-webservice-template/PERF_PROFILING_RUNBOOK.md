# Performance Profiling Runbook

Two-layer bottleneck triage, mirroring the Python template (py-spy) and the
Java template (async-profiler). Layer 1 answers *where* the time goes (every
run, report-only). Layer 2 answers *what kind* of time it is (on demand), with
Linux `perf` and an inferno flamegraph.

## Layer 1: where (Server-Timing + k6)

Every HTTP response carries a W3C `Server-Timing` header with per-boundary
durations plus a `total`, for example
`clickhouse_count;dur=1.20, clickhouse_select;dur=2.41, total;dur=4.05`.
Boundaries are recorded with `core::boundary_timing::timed(label, future)`,
inside a `tokio::task_local!` buffer scoped to the request, so background
work (the ingest task) never leaks into a response header. Labels are
rendered with `.` replaced by `_`, the token form the k6 parser expects.

Boundaries: `clickhouse.count`, `clickhouse.select` (GET /data); `lsm.query`
(GET /data/cache); `ingest.decode`, `ingest.lsm_write` (POST /data/ingest);
`postgres.config.get_all`, `postgres.config.set` (/config).

### Run it

k6 runs from the `perf-scripts` image (the same image as CI; no local k6
install). The app serves HTTPS with a self-signed certificate and the image
sets `K6_INSECURE_SKIP_TLS_VERIFY`, so `BASE_URL=https://app` works on the
compose network. The scripts are Python's, byte for byte
(`tests/cicd/test_perf_scripts.py`).

```bash
cd rust-webservice-template
docker build -t perf-scripts ./tests/performance

# Reads (ClickHouse + LSM cache):
docker compose up -d --build --wait
docker run --rm --network rust-template_default -e BASE_URL=https://app -e VUS=10 -e DURATION=60s perf-scripts run /scripts/profile_reads.js

# Ingest (Arrow decode + LSM write). The profiling overlay sets
# INGEST_TRANSPORT=noop, so HTTP is the sole LSM writer:
docker compose -f docker-compose.yml -f docker-compose.profiling.yml up -d --build --wait
docker run --rm --network rust-template_default -e BASE_URL=https://app -e VUS=4 -e DURATION=60s perf-scripts run /scripts/profile_ingest.js
```

The attribution table prints to the console. To also keep `attribution.json`
on the host, add `-v "$PWD/tests/performance:/scripts"` to the `docker run`.

### Read the output

Each endpoint prints a table ranked by mean time. `share` is the mean
fraction of total request time. `app_residual` is everything outside an
instrumented boundary: axum routing, the middleware stack, JSON
serialisation and tokio scheduling. The boundary (or the residual) with the
largest share is where Layer 2 should look.

## Layer 2: what kind (perf + flamegraph)

`cargo flamegraph` is a wrapper around `perf record` → `perf script` →
inferno. The runtime image has no cargo, so `run_perf.sh` runs perf inside
the app container and inferno on the host. The profiling overlay:
- adds `SYS_PTRACE`, plus `SYS_ADMIN` and `seccomp:unconfined`, which
  `perf_event_open` needs;
- rebuilds the app with frame pointers (`RUSTFLAGS=-C force-frame-pointers=yes`),
  so stacks are complete.

One-time host setup:
```bash
cargo install inferno --locked --version 0.12.8
```

Bring up the profiling stack, drive load in one shell, and sample in another.
`run_perf.sh` runs `perf stat` and then `perf record`, each for the given
duration, so keep the load running at least twice that long:

```bash
docker compose -f docker-compose.yml -f docker-compose.profiling.yml up -d --build --wait
```

Shell 1, drive load (140 s covers two 60 s passes):
```bash
docker run --rm --network rust-template_default -e BASE_URL=https://app -e VUS=10 -e DURATION=140s perf-scripts run /scripts/profile_reads.js
```

Shell 2, sample the app while the load runs:
```bash
tests/performance/profile/run_perf.sh 60
```

`run_perf.sh` is a bash script: on Windows, run it from Git Bash or WSL.
It installs `linux-perf` into the running container the first time, which is
ephemeral: re-run the script after a restart.

Artifacts land in `tests/performance/profile/artifacts/` (gitignored):
- `flame_cpu.svg`: the on-CPU flamegraph, showing where compute time goes;
- `perf-stat.txt`: `task-clock` and `CPUs utilized` over the window;
- `perf.folded`: collapsed stacks. Compare two runs with `inferno-diff-folded`.

### What works on Docker Desktop

Checked on Docker Desktop (WSL 2, kernel `7.0.14-linuxkit`) with perf 6.12
from Debian trixie, against a 20 s `profile_reads.js` load:
- `perf stat` and `perf record --call-graph fp` both run in the overlay's
  container. No fallback was needed.
- The VM exposes no hardware PMU, so perf samples the software `task-clock`
  event. Cycle and instruction counters are unavailable. On-CPU sampling and
  `CPUs utilized` still work.
- User-space stacks unwind fully through the frame-pointer build, from the
  thread entry point (`std::sys::thread::...::thread_start`) through tokio's
  `Harness::poll` to `rust_template::`, `axum`/`hyper`, `clickhouse` and
  `arrow_*` frames.
  Leaf samples inside glibc (`malloc`, `cfree`, `__madvise`) stop at
  `[libc.so.6]` or `[unknown]`, because Debian's glibc is built without frame
  pointers. If those towers matter, change `--call-graph fp` to
  `--call-graph dwarf` in `run_perf.sh`. That gives larger `perf.data`.
- Kernel frames stay unresolved. `kptr_restrict` hides `/proc/kallsyms`, and
  perf prints a warning about it that you can ignore. Off-CPU time (waiting on
  a socket) appears as a low `CPUs utilized` value, not as frames.
- Sample counts come in under the nominal 99 Hz (about 230 samples for 20 s at
  0.63 CPUs), so use 60 s windows for a readable flamegraph.

On Git Bash, `docker run ... /scripts/<file>.js` needs `MSYS_NO_PATHCONV=1`
in front of it. Without it, Git Bash rewrites `/scripts/...` into a Windows
path and k6 fails with `moduleSpecifier ... couldn't be found`.
`run_perf.sh` sets it itself.

## Decision rule

| Layer 1 says | perf shows | Verdict | Investigate |
|---|---|---|---|
| Large share in `clickhouse.*`, `postgres.*` or `lsm.query` | low `CPUs utilized` (well under 1); flamegraph dominated by tokio park/epoll and socket frames | **I/O-bound** on that store | pool sizes, query shape, indexing, network |
| Large `ingest.decode`, `ingest.lsm_write` or `app_residual` | `CPUs utilized` near the worker-thread count; towers in `arrow_*`, `rust_template::persistence::stream_store` or serde frames | **CPU-bound** in-process | batch sizes, merge/compaction thresholds (`LSM_FLUSH_ROWS`, `LSM_COMPACTION_RUNS`), the `benches/lsm.rs` benchmark |

There is no GIL: one tokio worker thread per core runs handlers in parallel,
while the LSM keeps a single writer behind a mutex. Under ingest load,
`CPUs utilized` close to 1 with towers under `ingest_batch` therefore means
the single writer is the limit, not the core count.

## Notes

- Layer 1 is report-only and never fails the build. Add per-boundary budgets
  if you want a regression gate.
- The ingest profile runs at modest concurrency by design. The LSM store is
  single-writer, so measuring write contention is a separate exercise.
- k6 reads `Server-Timing` straight from the response, so no CORS exposure
  configuration is needed.
- For the merge/compaction kernels alone, without HTTP, use the criterion
  benchmark: `cargo bench --bench lsm`.
