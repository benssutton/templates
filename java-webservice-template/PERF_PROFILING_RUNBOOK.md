# Performance Profiling Runbook

Two-layer bottleneck triage, mirroring the Python template (which uses py-spy);
here Layer 2 uses **async-profiler**, the JVM equivalent.

## Layer 1 — Where (Server-Timing + k6)

Every HTTP response carries a `Server-Timing` header with per-boundary durations
plus a `total`. The k6 profile scripts parse it into a ranked attribution table.

Boundaries: `clickhouse.count`, `clickhouse.select` (GET /data); `ingest.decode`,
`ingest.lsm_write` (POST /data/ingest). The Java service emits the same header
format as the Python one, so the reused k6 scripts work unchanged.

### Run it
k6 runs from the `perf-scripts` Docker image (same convention as CI); the app
serves HTTPS with a self-signed cert and the image sets
`K6_INSECURE_SKIP_TLS_VERIFY`, so `BASE_URL=https://app` works in-network.

```bash
cd java-webservice-template
docker compose up -d --build --wait
docker build -t perf-scripts ./tests/performance
docker run --rm --network java-template_default -e BASE_URL=https://app -e VUS=10 -e DURATION=60s perf-scripts run /scripts/profile_reads.js
```

## Layer 2 — What kind (async-profiler)

Requires the profiling overlay (adds `SYS_ADMIN`). Drive load in one shell and
sample in another so samples land under load.

```bash
docker compose -f docker-compose.yml -f docker-compose.profiling.yml up -d --build
# shell 1 — load:
docker run --rm --network java-template_default -e BASE_URL=https://app -e VUS=10 -e DURATION=140s perf-scripts run /scripts/profile_reads.js
# shell 2 — sample (Git Bash / WSL):
tests/performance/profile/run_asyncprofiler.sh 60
```

Artifacts in `tests/performance/profile/artifacts/`: `flame_wall.html` (overall
wall time) and `flame_cpu.html` (on-CPU only).

## Decision rule
| Layer 1 says | async-profiler shows | Verdict |
|---|---|---|
| Large share in `clickhouse.*` | sparse `flame_cpu.html`, threads parked in recv/await | **I/O-bound** on that store |
| Large `ingest.decode` / residual | heavy Java frames in `flame_cpu.html` (Arrow decode) | **CPU-bound** in-process |

If `flame_cpu.html` is a small fraction of `flame_wall.html`, the process is
waiting (I/O); if it is most of it, the process is computing.
