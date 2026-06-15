# Performance Profiling Runbook

Two-layer bottleneck triage. Layer 1 answers *where* the time goes (every run,
report-only). Layer 2 answers *what kind* of contention it is (on demand).

## Layer 1 — Where (Server-Timing + k6)

Each HTTP response carries a `Server-Timing` header with per-boundary durations
plus a `total`. The k6 profiles parse it into an attribution table.

Boundaries: `clickhouse.count`, `clickhouse.select` (GET /data); `lsm.query`
(GET /data/cache); `ingest.decode`, `ingest.lsm_write` (POST /data/ingest);
`postgres.config.get_all`, `postgres.config.set` (config paths).

### Run it

k6 runs from the `perf-scripts` Docker image — no local k6 install is needed
(same convention as CI and `GETTING_STARTED.md`). The app serves HTTPS with a
self-signed cert, and the image sets `K6_INSECURE_SKIP_TLS_VERIFY`, so
`BASE_URL=https://app` works in-network with no extra flags. Build it once:

```bash
docker build -t perf-scripts ./tests/performance
```

```bash
# Reads (ClickHouse + LSM cache):
docker compose up -d --build
docker run --rm --network python-template_default -e BASE_URL=https://app -e VUS=10 -e DURATION=60s perf-scripts run /scripts/profile_reads.js

# Ingest (Arrow decode + LSM write) — idle stream so HTTP is the sole writer:
docker compose -f docker-compose.yml -f docker-compose.profiling.yml up -d --build
docker run --rm --network python-template_default -e BASE_URL=https://app -e VUS=4 -e DURATION=60s perf-scripts run /scripts/profile_ingest.js
```

The attribution table prints to the console. To also persist `attribution.json`
to the host, mount the scripts dir into the run by adding
`-v "$PWD/tests/performance:/scripts"` (use `%cd%` instead of `$PWD` on Windows `cmd`).

### Read the output

Each endpoint prints a table ranked by mean time, and `attribution.json` is
written for the pipeline. `share` is the mean fraction of total request time;
`app_residual` is everything not inside an instrumented boundary
(framework, serialization, event-loop scheduling). The boundary (or the
residual) with the largest share is the area to inspect with Layer 2.

## Layer 2 — What kind (py-spy)

Requires the profiling stack (adds `SYS_PTRACE`). Bring it up, then run a k6
profile under load in one shell and the profiler in another so samples land
while the app is busy. `run_pyspy.sh` runs two sequential py-spy passes
(~2 × duration), so keep the load running at least that long:

```bash
docker compose -f docker-compose.yml -f docker-compose.profiling.yml up -d --build
```

Shell 1 — drive load (140s covers both 60s py-spy passes):

```bash
docker run --rm --network python-template_default -e BASE_URL=https://app -e VUS=10 -e DURATION=140s perf-scripts run /scripts/profile_reads.js
```

Shell 2 — sample the app while the load above runs:

```bash
tests/performance/profile/run_pyspy.sh 60
```

`run_pyspy.sh` is a bash script: on Windows run it from Git Bash (or WSL), not
`cmd`. In a single bash shell you can instead background the load with a
trailing `&` before launching the profiler.

Artifacts land in `tests/performance/profile/artifacts/`:
`flame_all.svg` (overall wall-time), `flame_gil.svg` (GIL-held only), and
`pyspy-dump.txt` (instantaneous thread states).

## Decision rule

| Layer 1 says | py-spy shows | Verdict | Investigate |
|---|---|---|---|
| Large share in `clickhouse.*` / `postgres.*` / `lsm.query` | threads parked in recv/await; sparse `flame_gil.svg` | **I/O-bound** on that store | pooling, query shape, indexing, network |
| Large `app_residual` (or `ingest.decode`) | heavy Python frames in `flame_gil.svg` (pyarrow decode, polars merge, pydantic) | **CPU/GIL-bound** in-process | offload to a thread / Rust, reduce work |

If `flame_gil.svg` is a small fraction of `flame_all.svg`, the process is
waiting (I/O); if it is most of it, the process is computing under the GIL.

## Notes

- Layer 1 is report-only: it never fails the build. Add per-boundary budgets
  later if you want a regression gate.
- The ingest profile runs at modest concurrency by design; the LSM store is
  single-writer, so high-concurrency ingest contention is a separate exercise.
- k6 reads `Server-Timing` directly from the response, so no CORS exposure
  configuration is required.
