# Performance Bottleneck Attribution — Design

**Date:** 2026-06-14
**Status:** Approved (design); pending implementation plan
**Scope:** `python-webservice-template`

## Goal

Give a developer (or the CI pipeline) a cheap, repeatable way to answer two
questions after a load test:

1. **Where** is the time going — main app, ClickHouse, Postgres, LSM store, or
   Arrow decode?
2. **What kind** of contention is it — CPU/GIL-bound or I/O-bound?

The output must point at the area of the codebase that warrants deeper
inspection. It is a triage tool, not a full APM.

## Principles alignment

- **Observability via standard mechanisms:** Layer 1 surfaces timings through a
  W3C `Server-Timing` response header — a standard HTTP mechanism — consumed by
  k6, the perf tool already in the repo.
- **Real services, real endpoints:** all measurement happens over real HTTP
  endpoints against the real docker-compose stack; tests drive real endpoints,
  no mocks.
- **Simple and transparent:** reuses the existing `timed()` context manager and
  k6 scripts. No new runtime dependencies on the request path. py-spy is an
  out-of-band CI tool, not a service dependency.
- **Isolation preserved:** boundary samples are request-scoped via a
  `ContextVar`, the same isolation pattern as the existing correlation ID, so
  the multi-app-per-process test model is unaffected.

## Two-layer architecture

### Layer 1 — Where (every run, report-only)

Server-side per-boundary wall-clock timing is attached to each HTTP response as
a `Server-Timing` header. k6 parses the header into per-boundary Trends and
prints a ranked attribution table per endpoint as its run summary, plus a JSON
artifact. Report-only: it never fails the build.

### Layer 2 — What kind (on demand)

A `py-spy` helper attaches to the running app process during a focused k6 run
and emits a flamegraph plus a GIL summary. A runbook converts those artifacts
into a CPU/GIL-vs-I/O verdict for the area Layer 1 flagged.

## Components and files

| File | Status | Responsibility |
|---|---|---|
| `core/boundary_timing.py` | new | Request-scoped `ContextVar[list[tuple[str, float]] \| None]`; `record_boundary(label, ms)`; `ServerTimingMiddleware`. |
| `core/correlation.py` | modify | `timed()` additionally calls `record_boundary(label, elapsed_ms)`. |
| `services/stream_ingest.py` | modify | Wrap `lsm.query` in `get_data`; wrap `ingest.lsm_write` in `ingest_batch`. |
| `routers/data.py` | modify | Wrap `ingest.decode` (pyarrow IPC) separately from the write. |
| `main.py` | modify | Register `ServerTimingMiddleware`. |
| `tests/performance/lib/serverTiming.js` | new | Parse `Server-Timing` into Trends; `handleSummary` builds the attribution table and writes `attribution.json`. |
| `tests/performance/profile_reads.js` | new | k6 scenario: GET `/data` + GET `/data/cache`. |
| `tests/performance/profile_ingest.js` | new | k6 scenario: POST `/data/ingest` against an idle stream source, modest concurrency. |
| `tests/performance/profile/run_pyspy.sh` | new | Attach py-spy to the app container; emit flamegraph + `--gil` summary. |
| `docker-compose.profiling.yml` | new | Override adding `SYS_PTRACE` cap and the idle-stream env for the ingest profile. |
| `docs/superpowers/perf-profiling-runbook.md` | new | How to run both layers; the CPU/GIL-vs-I/O decision rule. |
| `tests/test_server_timing.py` | new | Integration test: header present and correct; streaming does not pollute it. |
| `CLAUDE.md` | modify | Document the perf-profiling tooling. |

### The seven boundaries

| Boundary label | Site | Path |
|---|---|---|
| `clickhouse.count` | `DataService.get_data` (existing) | GET `/data` |
| `clickhouse.select` | `DataService.get_data` (existing) | GET `/data` |
| `postgres.config.get_all` | `ConfigService.get_all` (existing) | (config paths) |
| `postgres.config.set` | `ConfigService.set` (existing) | (config paths) |
| `lsm.query` | `StreamIngestService.get_data` (new) | GET `/data/cache` |
| `ingest.decode` | `routers/data.py::ingest_batch` (new) | POST `/data/ingest` |
| `ingest.lsm_write` | `StreamIngestService.ingest_batch` (new) | POST `/data/ingest` |

One change to `timed()` lights up all seven; the new boundaries simply adopt the
existing `async with timed(...)` form.

## Layer 1 data flow (server side)

1. `ServerTimingMiddleware` runs inside the correlation middleware. On each HTTP
   request it sets a fresh sample list on the boundary `ContextVar`, records the
   start time, and calls the downstream handler.
2. Each `timed()` block appends `(label, elapsed_ms)` to the active list. If no
   list is active (non-HTTP callers such as the streaming ingest thread), it
   only logs — unchanged from today.
3. On the way out the middleware renders the samples plus a `total` entry into a
   `Server-Timing` header, e.g.
   `Server-Timing: clickhouse_select;dur=12.3, clickhouse_count;dur=1.1, total;dur=15.0`,
   then resets the `ContextVar` token in a `finally`.
4. Labels are sanitized for the header grammar: `.` becomes `_`. The k6 side maps
   them back for display.

Request-scoping guarantees: (a) background streaming writes never appear in HTTP
boundary samples; (b) multiple isolated apps in one test process cannot collide,
because each request carries its own list in its own context.

No service constructors change. No Prometheus registry is involved.

## Layer 1 k6 side

- `profile_reads.js` and `profile_ingest.js` reuse the existing executors and SLO
  presets from `tests/performance/lib/`. Each targets one path-group.
- After every response, the shared helper pushes each `Server-Timing` entry into
  a named Trend (`b_clickhouse_select`, `b_lsm_query`, `total`, ...).
- A shared `handleSummary` computes, per endpoint: `total` p95 and each
  boundary's share of `total`. The **residual** (`total − Σ boundaries`) is
  reported as `app_residual` — the framework / serialization / event-loop bucket.
- Output: a ranked table to stdout and a machine-readable `attribution.json`.
- **Report-only.** The existing `http_req_failed` / `http_req_duration` SLO
  thresholds are unchanged; no boundary threshold fails the build.

### Ingest profile isolation

`POST /data/ingest` and the streaming consumer both call `_record_ingest` →
`store.ingest`, and the LSM store is single-writer by contract. The ingest
profile therefore runs the app against an **idle flight source** (empty stream
script, as the test `empty_flight_server` does) so HTTP is the sole writer, and
at **modest concurrency** so decode-vs-write attribution stays clean. Driving
ingest concurrency high to probe single-writer contention is a deliberate later
follow-up, out of scope here.

## Layer 2 (py-spy)

`run_pyspy.sh <duration>` locates the app PID inside its container and runs:

- `py-spy record` → flamegraph SVG (where in the call stack the samples land);
- a `py-spy top` / `--gil` capture → percentage of time holding the GIL.

Both land in an artifacts directory. Requires `SYS_PTRACE`, added only in
`docker-compose.profiling.yml`.

### Decision rule (documented in the runbook)

- Layer 1 points at a `clickhouse.*` / `postgres.*` / `lsm.*` boundary **and**
  py-spy shows threads parked in recv/await (low GIL-held, low on-CPU) →
  **I/O-bound** on that store. Investigate pooling, query shape, indexing.
- Layer 1 shows a large `app_residual` **and** py-spy shows high GIL-held % on
  Python frames (e.g. `ingest.decode` pyarrow, polars merge, pydantic
  serialization) → **CPU/GIL-bound** in-process. Investigate offloading to a
  thread / Rust, or reducing the work.

## Error handling

- `ServerTimingMiddleware` never raises into the response path. If header
  rendering fails it logs and omits the header — timing is diagnostic, never
  load-bearing.
- The k6 parser tolerates a missing or malformed `Server-Timing` header,
  treating boundaries as absent rather than failing the run.
- `record_boundary` is a no-op when no sample list is active, so non-HTTP call
  sites are unaffected.

## Testing

- **`tests/test_server_timing.py`** (real endpoints, real containers):
  - GET `/data` response carries a `Server-Timing` header containing
    `clickhouse_select` and `total`.
  - A streaming-only ingest (no HTTP request) produces no header pollution —
    verifying request-scoped isolation.
- k6 scripts are exercised by running the profiles against the docker-compose
  stack and confirming `attribution.json` is produced with the expected
  boundary keys.

## Out of scope

- Failing the build on boundary budgets (report-only by decision; budgets can be
  layered on later without rework).
- High-concurrency ingest contention probing of the single-writer LSM store.
- Historical dashboards (the data lives in per-run k6 artifacts, not Prometheus).
- Any fix for bottlenecks the tool surfaces — this project builds the tool only.
