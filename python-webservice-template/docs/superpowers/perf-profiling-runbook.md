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

```bash
# Reads (ClickHouse + LSM cache):
docker compose up -d --build
k6 run -e VUS=10 -e DURATION=60s tests/performance/profile_reads.js

# Ingest (Arrow decode + LSM write) — idle stream so HTTP is the sole writer:
docker compose -f docker-compose.yml -f docker-compose.profiling.yml up -d --build
k6 run -e VUS=4 -e DURATION=60s tests/performance/profile_ingest.js
```

### Read the output

Each endpoint prints a table ranked by mean time, and `attribution.json` is
written for the pipeline. `share` is the mean fraction of total request time;
`app_residual` is everything not inside an instrumented boundary
(framework, serialization, event-loop scheduling). The boundary (or the
residual) with the largest share is the area to inspect with Layer 2.

## Layer 2 — What kind (py-spy)

Requires the profiling stack (adds `SYS_PTRACE`). Run a k6 profile in one shell
and the profiler in another so samples land under load:

```bash
docker compose -f docker-compose.yml -f docker-compose.profiling.yml up -d --build
k6 run -e VUS=10 -e DURATION=70s tests/performance/profile_reads.js &
tests/performance/profile/run_pyspy.sh 60
```

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
