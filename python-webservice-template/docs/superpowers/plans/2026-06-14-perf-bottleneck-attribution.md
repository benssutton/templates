# Performance Bottleneck Attribution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give developers and CI a cheap, repeatable way to attribute request latency to a code area (Layer 1, every run) and classify the contention as CPU/GIL- vs I/O-bound (Layer 2, on demand).

**Architecture:** A pure-ASGI middleware installs a request-scoped `ContextVar` sample list; the existing `timed()` context manager appends `(label, ms)` to it; the middleware renders a W3C `Server-Timing` response header. k6 parses that header into per-boundary Trends and prints a ranked attribution table plus a JSON artifact. A `py-spy` helper + runbook handles the on-demand contention classification.

**Tech Stack:** Python 3.12, FastAPI/Starlette (pure-ASGI middleware), `contextvars`, pytest + testcontainers, k6 (JavaScript), py-spy, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-06-14-perf-bottleneck-attribution-design.md`

**Test runner note:** the project interpreter is the conda env `p312`. If `pytest` is not on PATH, invoke it as `C:/Users/Alexander/miniconda3/envs/p312/python.exe -m pytest ...`. Commands below use plain `pytest` for brevity.

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `core/boundary_timing.py` | new | Request-scoped sample `ContextVar`, `record_boundary()`, `_render_header()`, pure-ASGI `ServerTimingMiddleware`. |
| `core/correlation.py` | modify | `timed()` appends each boundary's duration via `record_boundary()`. |
| `services/stream_ingest.py` | modify | Wrap `lsm.query` (`get_data`) and `ingest.lsm_write` (`ingest_batch`). |
| `routers/data.py` | modify | Wrap `ingest.decode` (pyarrow IPC) separately from the write. |
| `main.py` | modify | Register `ServerTimingMiddleware`. |
| `tests/test_boundary_timing.py` | new | Unit tests for `record_boundary` / `_render_header`. |
| `tests/test_correlation.py` | modify | Test that `timed()` records a boundary sample. |
| `tests/test_server_timing.py` | new | Integration tests: header present/correct; request-scoped isolation. |
| `tests/performance/lib/serverTiming.js` | new | Parse `Server-Timing` into Trends; build attribution table + JSON. |
| `tests/performance/profile_reads.js` | new | k6 read profile: GET `/data` + GET `/data/cache`. |
| `tests/performance/profile_ingest.js` | new | k6 ingest profile: POST `/data/ingest` (idle stream, modest concurrency). |
| `docker-compose.profiling.yml` | new | Override: `SYS_PTRACE` cap + idle-stream env for the ingest profile. |
| `tests/performance/profile/run_pyspy.sh` | new | Attach py-spy to the app container; emit flamegraphs + dump. |
| `docs/superpowers/perf-profiling-runbook.md` | new | How to run both layers; the CPU/GIL-vs-I/O decision rule. |
| `CLAUDE.md` | modify | Document the perf-profiling tooling. |

**The seven boundaries** (label → site): `clickhouse.count`, `clickhouse.select` (`DataService.get_data`, existing); `postgres.config.get_all`, `postgres.config.set` (`ConfigService`, existing); `lsm.query` (`StreamIngestService.get_data`, new); `ingest.decode` (`routers/data.py`, new); `ingest.lsm_write` (`StreamIngestService.ingest_batch`, new).

---

## Task 1: Boundary capture core + Server-Timing middleware

**Files:**
- Create: `core/boundary_timing.py`
- Test: `tests/test_boundary_timing.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_boundary_timing.py`:

```python
from core.boundary_timing import (
    _render_header,
    boundary_samples_var,
    record_boundary,
)


def test_record_boundary_appends_when_list_active():
    token = boundary_samples_var.set([])
    try:
        record_boundary("clickhouse.select", 12.3)
        record_boundary("clickhouse.count", 1.1)
        assert boundary_samples_var.get() == [
            ("clickhouse.select", 12.3),
            ("clickhouse.count", 1.1),
        ]
    finally:
        boundary_samples_var.reset(token)


def test_record_boundary_is_noop_when_inactive():
    # Outside a request the ContextVar default is None; recording must not raise.
    assert boundary_samples_var.get() is None
    record_boundary("clickhouse.select", 5.0)
    assert boundary_samples_var.get() is None


def test_render_header_sums_duplicates_and_sanitizes_labels():
    header = _render_header(
        [("clickhouse.select", 10.0), ("clickhouse.select", 2.5), ("lsm.query", 4.0)],
        total_ms=20.0,
    )
    # dotted labels -> tokens; duplicate labels summed; `total` appended last.
    assert header == "clickhouse_select;dur=12.50, lsm_query;dur=4.00, total;dur=20.00"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_boundary_timing.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'core.boundary_timing'`.

- [ ] **Step 3: Write the implementation**

Create `core/boundary_timing.py`:

```python
"""Request-scoped capture of per-boundary wall-clock timings, surfaced as a
W3C Server-Timing response header.

ServerTimingMiddleware installs a fresh sample list on a ContextVar at the start
of each HTTP request; core.correlation.timed() appends (label, milliseconds) to
that list as each instrumented boundary completes. On the response's
`http.response.start` the middleware renders the samples — plus a synthetic
`total` for the whole handler — into a Server-Timing header.

Request-scoping (the same isolation model as the correlation ID) means work that
runs outside a request — e.g. the streaming ingest thread — never pollutes a
request's samples, and multiple isolated apps in one test process cannot collide.

Implemented as pure ASGI (mirroring core.request_limits.MaxBodySizeMiddleware) so
it does not buffer streaming responses (the /mcp mount) and propagates the
ContextVar to the endpoint reliably.
"""
import contextvars
import logging
import re
import time

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

log = logging.getLogger(__name__)

# None when no HTTP request is in flight (e.g. the streaming ingest thread):
# record_boundary becomes a no-op and timed() only logs, exactly as before.
boundary_samples_var: contextvars.ContextVar[list[tuple[str, float]] | None] = (
    contextvars.ContextVar("boundary_samples", default=None)
)

# Server-Timing metric names must be tokens; map any other char to underscore.
_TOKEN_RE = re.compile(r"[^A-Za-z0-9_]")


def record_boundary(label: str, milliseconds: float) -> None:
    """Append a boundary sample to the current request's list, if one is active.

    No-op outside an HTTP request (the list is None), so non-request callers such
    as the streaming ingest thread are unaffected."""
    samples = boundary_samples_var.get()
    if samples is not None:
        samples.append((label, milliseconds))


def _render_header(samples: list[tuple[str, float]], total_ms: float) -> str:
    """Render samples into a Server-Timing value, summing duplicate labels (e.g.
    one ingest.lsm_write per batch) and preserving first-seen order."""
    aggregated: dict[str, float] = {}
    order: list[str] = []
    for label, ms in samples:
        token = _TOKEN_RE.sub("_", label)
        if token not in aggregated:
            order.append(token)
            aggregated[token] = 0.0
        aggregated[token] += ms
    parts = [f"{token};dur={aggregated[token]:.2f}" for token in order]
    parts.append(f"total;dur={total_ms:.2f}")
    return ", ".join(parts)


class ServerTimingMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        token = boundary_samples_var.set([])
        start = time.perf_counter()

        async def send_wrapper(message: Message) -> None:
            if message["type"] == "http.response.start":
                total_ms = (time.perf_counter() - start) * 1000
                samples = boundary_samples_var.get() or []
                try:
                    MutableHeaders(scope=message)["Server-Timing"] = _render_header(
                        samples, total_ms
                    )
                except Exception:  # diagnostics must never break the response
                    log.exception("failed to render Server-Timing header")
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            boundary_samples_var.reset(token)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_boundary_timing.py -v`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
git add core/boundary_timing.py tests/test_boundary_timing.py
git commit -m "feat: request-scoped boundary capture + Server-Timing middleware

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: Wire `timed()` to record boundary samples

**Files:**
- Modify: `core/correlation.py:73-83`
- Test: `tests/test_correlation.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_correlation.py`:

```python
@pytest.mark.asyncio
async def test_timed_records_boundary_sample_when_active():
    from core.boundary_timing import boundary_samples_var
    from core.correlation import timed

    token = boundary_samples_var.set([])
    try:
        async with timed("clickhouse.select"):
            pass
        samples = boundary_samples_var.get()
        assert len(samples) == 1
        assert samples[0][0] == "clickhouse.select"
        assert samples[0][1] >= 0.0
    finally:
        boundary_samples_var.reset(token)


@pytest.mark.asyncio
async def test_timed_is_silent_when_no_request_active():
    from core.boundary_timing import boundary_samples_var
    from core.correlation import timed

    assert boundary_samples_var.get() is None
    async with timed("clickhouse.select"):   # must not raise
        pass
    assert boundary_samples_var.get() is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_correlation.py::test_timed_records_boundary_sample_when_active -v`
Expected: FAIL with `AssertionError` (no sample appended — `timed()` does not record yet).

- [ ] **Step 3: Modify `timed()`**

In `core/correlation.py`, add the import near the top (after the existing stdlib imports, around line 14):

```python
from core.boundary_timing import record_boundary
```

Replace the `timed` context manager body (currently lines 73-83) with:

```python
@asynccontextmanager
async def timed(label: str):
    """Log the wall-clock duration of an awaited boundary, tagged with the
    current correlation ID (added by CorrelationIdFilter), and record it as a
    Server-Timing sample for the current request (added by record_boundary).
    Structured as a context manager so an OpenTelemetry span could wrap the same
    boundary later without changing call sites."""
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed_ms = (time.perf_counter() - start) * 1000
        _timing_log.debug("%s %.2fms", label, elapsed_ms)
        record_boundary(label, elapsed_ms)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_correlation.py -v`
Expected: PASS (all existing correlation tests plus the two new ones).

- [ ] **Step 5: Commit**

```bash
git add core/correlation.py tests/test_correlation.py
git commit -m "feat: timed() records boundary samples for Server-Timing

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: Register the middleware and verify end-to-end

**Files:**
- Modify: `main.py:108-117`
- Test: `tests/test_server_timing.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_server_timing.py`:

```python
import pytest

pytestmark = pytest.mark.observability


async def test_data_read_emits_server_timing(test_client):
    r = await test_client.get("/data?limit=5")
    assert r.status_code == 200
    server_timing = r.headers.get("Server-Timing", "")
    assert "clickhouse_select" in server_timing
    assert "total" in server_timing


async def test_request_without_boundaries_has_only_total(test_client):
    # /health/live has no instrumented boundaries; its Server-Timing must carry
    # only `total`, proving background streaming writes never leak into a
    # request's samples (request-scoped ContextVar isolation).
    r = await test_client.get("/health/live")
    assert r.status_code == 200
    server_timing = r.headers.get("Server-Timing", "")
    assert "total" in server_timing
    for token in ("clickhouse", "lsm", "ingest", "postgres"):
        assert token not in server_timing
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_server_timing.py -v`
Expected: FAIL — no `Server-Timing` header yet (`assert "clickhouse_select" in ""`).

- [ ] **Step 3: Register the middleware**

In `main.py`, add the import alongside the other core imports (after line 13):

```python
from core.boundary_timing import ServerTimingMiddleware
```

Then in `create_app`, add `ServerTimingMiddleware` as the **first** `add_middleware` call so it is the innermost of that group (closest to the endpoint). Replace lines 108-117:

```python
    app.add_middleware(ServerTimingMiddleware)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_allow_origins,
        allow_methods=settings.cors_allow_methods,
        allow_headers=settings.cors_allow_headers,
        allow_credentials=settings.cors_allow_credentials,
    )
    app.add_middleware(CorrelationIdMiddleware, header_name=settings.correlation_id_header)
    # MaxBodySizeMiddleware must be LAST (outermost) — Starlette LIFO ordering.
    app.add_middleware(MaxBodySizeMiddleware, max_bytes=settings.max_request_body_bytes)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_server_timing.py -v`
Expected: PASS (2 passed). `clickhouse.select` is already wrapped in `DataService.get_data`, so the `/data` boundary appears without further changes.

- [ ] **Step 5: Run the broader suite to confirm no regression**

Run: `pytest tests/test_observability.py tests/test_correlation.py tests/test_cors.py -v`
Expected: PASS — adding a response header does not disturb existing assertions.

- [ ] **Step 6: Commit**

```bash
git add main.py tests/test_server_timing.py
git commit -m "feat: emit Server-Timing header via ServerTimingMiddleware

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: Instrument the three new boundaries

**Files:**
- Modify: `services/stream_ingest.py:13` (import), `:166-170` (`get_data`), `:172-173` (`ingest_batch`)
- Modify: `routers/data.py:4` (import), `:36-48` (`ingest_batch`)
- Test: `tests/test_server_timing.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_server_timing.py`:

```python
async def test_cache_read_emits_lsm_boundary(test_client):
    r = await test_client.get("/data/cache?limit=5")
    assert r.status_code == 200
    assert "lsm_query" in r.headers.get("Server-Timing", "")


async def test_http_ingest_emits_decode_and_write_boundaries(test_client):
    import pyarrow as pa
    import pyarrow.ipc as pa_ipc
    from tests.publishers.flight_server import make_batch

    batch = make_batch([(900, "perf", "v1", "upsert")])
    buf = pa.BufferOutputStream()
    with pa_ipc.new_stream(buf, batch.schema) as writer:
        writer.write_batch(batch)
    r = await test_client.post(
        "/data/ingest",
        content=buf.getvalue().to_pybytes(),
        headers={"Content-Type": "application/vnd.apache.arrow.stream"},
    )
    assert r.status_code == 202
    server_timing = r.headers.get("Server-Timing", "")
    assert "ingest_decode" in server_timing
    assert "ingest_lsm_write" in server_timing
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_server_timing.py::test_cache_read_emits_lsm_boundary tests/test_server_timing.py::test_http_ingest_emits_decode_and_write_boundaries -v`
Expected: FAIL — `lsm_query` / `ingest_decode` / `ingest_lsm_write` are not yet recorded.

- [ ] **Step 3: Instrument `stream_ingest.py`**

In `services/stream_ingest.py`, change the correlation import (line 13) from:

```python
from core.correlation import new_id, set_correlation_id
```

to:

```python
from core.correlation import new_id, set_correlation_id, timed
```

Replace `get_data` (lines 166-170):

```python
    async def get_data(self, limit: int) -> DataRowsResponse:
        async with timed("lsm.query"):
            rows, total = await asyncio.to_thread(self._store.query, limit)
        return DataRowsResponse(
            rows=[DataRowResponse(**r) for r in rows], total=total, limit=limit
        )
```

Replace `ingest_batch` (lines 172-173):

```python
    async def ingest_batch(self, batch: pa.RecordBatch) -> None:
        async with timed("ingest.lsm_write"):
            await asyncio.to_thread(self._record_ingest, batch)
```

- [ ] **Step 4: Instrument `routers/data.py`**

In `routers/data.py`, add the import after line 3 (`import pyarrow as pa`):

```python
from core.correlation import timed
```

Replace the `ingest_batch` route (lines 36-48) so decode is timed separately from the write:

```python
@router.post("/ingest", status_code=202)
async def ingest_batch(
    request: Request,
    svc: StreamIngestServiceDep,
) -> dict:
    body = await request.body()
    try:
        async with timed("ingest.decode"):
            reader = pa.ipc.open_stream(pa.BufferReader(body))
            batches = list(reader)        # force full decode inside the timed block
    except pa.ArrowInvalid as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    for batch in batches:
        await svc.ingest_batch(batch)
    return {"accepted": True}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/test_server_timing.py -v`
Expected: PASS (4 passed).

- [ ] **Step 6: Run the ingest and data suites to confirm no regression**

Run: `pytest tests/test_http_ingest.py tests/test_data.py tests/test_flight_cache.py -v`
Expected: PASS — decode/write semantics are unchanged; only timing wrappers were added.

- [ ] **Step 7: Commit**

```bash
git add services/stream_ingest.py routers/data.py tests/test_server_timing.py
git commit -m "feat: instrument lsm.query, ingest.decode, ingest.lsm_write boundaries

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 5: k6 Layer 1 — parsing library and profile scripts

**Files:**
- Create: `tests/performance/lib/serverTiming.js`
- Create: `tests/performance/profile_reads.js`
- Create: `tests/performance/profile_ingest.js`

This task produces k6 scripts. k6 scripts are validated by `k6 archive` (offline parse/compile check) and by running against the docker-compose stack — there is no in-repo JS unit harness.

- [ ] **Step 1: Create the parsing/attribution library**

Create `tests/performance/lib/serverTiming.js`:

```javascript
import { Trend } from 'k6/metrics';

// Boundaries expected per endpoint key. Trends must be created in the init
// context (module scope), so the full endpoint x label set is pre-declared.
export const ENDPOINTS = {
  data: ['clickhouse_count', 'clickhouse_select', 'total'],
  cache: ['lsm_query', 'total'],
  ingest: ['ingest_decode', 'ingest_lsm_write', 'total'],
};

const _trends = {};
for (const [endpoint, labels] of Object.entries(ENDPOINTS)) {
  for (const label of labels) {
    _trends[`${endpoint}.${label}`] = new Trend(`st_${endpoint}_${label}`, true);
  }
}

// "clickhouse_select;dur=12.30, total;dur=15.00" -> { clickhouse_select: 12.3, total: 15.0 }
export function parseServerTiming(header) {
  const out = {};
  if (!header) return out;
  for (const part of header.split(',')) {
    const m = part.trim().match(/^([A-Za-z0-9_]+);dur=([0-9.]+)/);
    if (m) out[m[1]] = parseFloat(m[2]);
  }
  return out;
}

// Record one response's Server-Timing entries into the named endpoint's Trends.
export function recordServerTiming(res, endpoint) {
  const parsed = parseServerTiming(res.headers['Server-Timing']);
  for (const [label, ms] of Object.entries(parsed)) {
    const trend = _trends[`${endpoint}.${label}`];
    if (trend) trend.add(ms);
  }
}

function metricVal(data, name, key) {
  const m = data.metrics[name];
  if (!m || !m.values || m.values[key] === undefined) return null;
  return m.values[key];
}

// Mean-based shares are additive (E[total] = sum E[boundary] + E[residual]); the
// residual is the framework/serialization/event-loop bucket. p95 is reported per
// boundary for tail awareness.
function attributionFor(data, endpoint) {
  const meanTotal = metricVal(data, `st_${endpoint}_total`, 'avg');
  if (meanTotal === null) return null;   // endpoint not exercised this run
  const labels = ENDPOINTS[endpoint].filter((l) => l !== 'total');
  const rows = [];
  let meanSum = 0;
  for (const label of labels) {
    const mean = metricVal(data, `st_${endpoint}_${label}`, 'avg');
    if (mean === null) continue;
    meanSum += mean;
    rows.push({
      boundary: label,
      mean_ms: round2(mean),
      p95_ms: round2(metricVal(data, `st_${endpoint}_${label}`, 'p(95)')),
      share: round3(mean / meanTotal),
    });
  }
  const residual = Math.max(meanTotal - meanSum, 0);
  rows.push({ boundary: 'app_residual', mean_ms: round2(residual), p95_ms: null, share: round3(residual / meanTotal) });
  rows.sort((a, b) => b.mean_ms - a.mean_ms);
  return {
    endpoint,
    mean_total_ms: round2(meanTotal),
    p95_total_ms: round2(metricVal(data, `st_${endpoint}_total`, 'p(95)')),
    boundaries: rows,
  };
}

function round2(x) { return x === null ? null : Math.round(x * 100) / 100; }
function round3(x) { return x === null ? null : Math.round(x * 1000) / 1000; }

function renderTable(attr) {
  let out = `\n=== ${attr.endpoint}  (mean total ${attr.mean_total_ms}ms, p95 ${attr.p95_total_ms}ms) ===\n`;
  out += '  boundary             mean_ms   p95_ms   share\n';
  for (const r of attr.boundaries) {
    const p95 = r.p95_ms === null ? '-' : String(r.p95_ms);
    out += `  ${r.boundary.padEnd(20)} ${String(r.mean_ms).padStart(7)} ${p95.padStart(8)} ${(r.share * 100).toFixed(1).padStart(6)}%\n`;
  }
  return out;
}

// handleSummary entry point: builds tables for every exercised endpoint and
// writes attribution.json. Report-only — never alters thresholds/exit code.
export function summarize(data) {
  const report = { generated_at: new Date().toISOString(), endpoints: [] };
  let text = '';
  for (const endpoint of Object.keys(ENDPOINTS)) {
    const attr = attributionFor(data, endpoint);
    if (attr) {
      report.endpoints.push(attr);
      text += renderTable(attr);
    }
  }
  return {
    stdout: text + '\n',
    'attribution.json': JSON.stringify(report, null, 2),
  };
}
```

- [ ] **Step 2: Create the read profile**

Create `tests/performance/profile_reads.js`:

```javascript
import http from 'k6/http';
import { recordServerTiming, summarize } from './lib/serverTiming.js';
import { NORMAL_SLO } from './lib/thresholds.js';

const BASE = __ENV.BASE_URL || 'http://localhost:8000';

export const options = {
  scenarios: {
    reads: {
      executor: 'constant-vus',
      vus: parseInt(__ENV.VUS || '10', 10),
      duration: __ENV.DURATION || '60s',
    },
  },
  thresholds: { ...NORMAL_SLO },   // report-only attribution; SLOs unchanged
};

export default function () {
  recordServerTiming(http.get(`${BASE}/data?limit=10`), 'data');
  recordServerTiming(http.get(`${BASE}/data/cache?limit=10`), 'cache');
}

export function handleSummary(data) {
  return summarize(data);
}
```

- [ ] **Step 3: Create the ingest profile**

Create `tests/performance/profile_ingest.js`:

```javascript
import http from 'k6/http';
import { recordServerTiming, summarize } from './lib/serverTiming.js';
import { RELAXED_SLO } from './lib/thresholds.js';

const BASE = __ENV.BASE_URL || 'http://localhost:8000';
// Pre-generated Arrow IPC batch fixture (see tests/performance/data/).
const PAYLOAD = open('./data/ingest_batch.ipc', 'b');

export const options = {
  scenarios: {
    ingest: {
      executor: 'constant-vus',
      // Modest concurrency: the LSM store is single-writer, so this profile
      // measures decode-vs-write attribution rather than write contention.
      // Run against an idle stream source (docker-compose.profiling.yml) so
      // HTTP ingest is the sole LSM writer.
      vus: parseInt(__ENV.VUS || '4', 10),
      duration: __ENV.DURATION || '60s',
    },
  },
  thresholds: { ...RELAXED_SLO },
};

export default function () {
  const res = http.post(`${BASE}/data/ingest`, PAYLOAD, {
    headers: { 'Content-Type': 'application/vnd.apache.arrow.stream' },
  });
  recordServerTiming(res, 'ingest');
}

export function handleSummary(data) {
  return summarize(data);
}
```

- [ ] **Step 4: Validate the scripts parse/compile offline**

Run (from `tests/performance/`):

```bash
k6 archive profile_reads.js -O /tmp/reads.tar
k6 archive profile_ingest.js -O /tmp/ingest.tar
```

Expected: both commands exit 0 with no syntax/compile error (this validates the JS and the `./lib/serverTiming.js` / `./data/ingest_batch.ipc` references resolve). If `k6` is not installed, install it (https://grafana.com/docs/k6/latest/set-up/install-k6/) or use the image built by `tests/performance/Dockerfile`.

- [ ] **Step 5: Validate against a running stack (produces the artifact)**

Bring the stack up (base compose is enough for the read profile) and run a short profile:

```bash
docker compose up -d --build
k6 run -e VUS=5 -e DURATION=15s tests/performance/profile_reads.js
```

Expected: a `=== data ...` and `=== cache ...` table on stdout and an `attribution.json` written to the working directory, with `boundaries` entries including `clickhouse_select`, `lsm_query`, and `app_residual`.

- [ ] **Step 6: Commit**

```bash
git add tests/performance/lib/serverTiming.js tests/performance/profile_reads.js tests/performance/profile_ingest.js
git commit -m "feat: k6 Server-Timing attribution library and read/ingest profiles

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 6: Layer 2 — profiling compose override and py-spy helper

**Files:**
- Create: `docker-compose.profiling.yml`
- Create: `tests/performance/profile/run_pyspy.sh`

- [ ] **Step 1: Create the profiling compose override**

Create `docker-compose.profiling.yml`:

```yaml
# Profiling override. Use alongside the base file:
#   docker compose -f docker-compose.yml -f docker-compose.profiling.yml up -d --build
#
# - SYS_PTRACE lets py-spy attach to the app process (Layer 2).
# - FLIGHT_INTERVAL is raised so the stream source is effectively idle, making
#   HTTP ingest the sole LSM writer during the ingest profile (single-writer
#   contract) — see tests/performance/profile_ingest.js.
services:
  app:
    cap_add:
      - SYS_PTRACE
  flight:
    environment:
      FLIGHT_INTERVAL: "3600"
```

- [ ] **Step 2: Validate the override merges**

Run:

```bash
docker compose -f docker-compose.yml -f docker-compose.profiling.yml config
```

Expected: exit 0; the rendered config shows `cap_add: [SYS_PTRACE]` under `app` and `FLIGHT_INTERVAL: "3600"` under `flight`.

- [ ] **Step 3: Create the py-spy helper**

Create `tests/performance/profile/run_pyspy.sh`:

```bash
#!/usr/bin/env bash
# Attach py-spy to the running app container and capture profiles for Layer 2
# contention classification (CPU/GIL vs I/O). Run a k6 profile concurrently so
# the samples land under load.
#
# Usage: tests/performance/profile/run_pyspy.sh [DURATION_SECONDS]
# Requires the profiling stack (docker-compose.profiling.yml) so the app
# container has the SYS_PTRACE capability.
set -euo pipefail

DURATION="${1:-30}"
OUT_DIR="$(cd "$(dirname "$0")" && pwd)/artifacts"
mkdir -p "$OUT_DIR"

APP_CID="$(docker compose ps -q app)"
if [ -z "$APP_CID" ]; then
  echo "app container not running. Start the profiling stack first:" >&2
  echo "  docker compose -f docker-compose.yml -f docker-compose.profiling.yml up -d --build" >&2
  exit 1
fi

# py-spy ships manylinux wheels, so pip install needs no compiler.
docker exec "$APP_CID" pip install --quiet py-spy

# The app is the container's main process; fall back to PID 1.
APP_PID="$(docker exec "$APP_CID" sh -c "pgrep -f 'main.py|uvicorn|main:app' | head -n1 || echo 1")"
echo "Profiling app pid=$APP_PID for ${DURATION}s..."

# (1) Full flamegraph including threads parked in I/O (--idle): shows where wall
#     time goes overall.
docker exec "$APP_CID" py-spy record --pid "$APP_PID" --duration "$DURATION" \
  --idle --format flamegraph --output /tmp/flame_all.svg
docker cp "$APP_CID:/tmp/flame_all.svg" "$OUT_DIR/flame_all.svg"

# (2) GIL-only flamegraph (--gil): samples only threads holding the GIL. Heavy
#     mass here on Python frames => CPU/GIL-bound; sparse => I/O-bound.
docker exec "$APP_CID" py-spy record --pid "$APP_PID" --duration "$DURATION" \
  --gil --format flamegraph --output /tmp/flame_gil.svg
docker cp "$APP_CID:/tmp/flame_gil.svg" "$OUT_DIR/flame_gil.svg"

# (3) One-shot thread dump: shows what each thread is doing right now (e.g.
#     socket recv vs Python CPU frame).
docker exec "$APP_CID" py-spy dump --pid "$APP_PID" > "$OUT_DIR/pyspy-dump.txt" || true

echo "Artifacts written to $OUT_DIR:"
echo "  flame_all.svg   — overall wall-time flamegraph"
echo "  flame_gil.svg   — GIL-held flamegraph (CPU/GIL signal)"
echo "  pyspy-dump.txt  — instantaneous thread states"
echo "Interpret with docs/superpowers/perf-profiling-runbook.md."
```

- [ ] **Step 4: Validate shell syntax**

Run:

```bash
bash -n tests/performance/profile/run_pyspy.sh
```

Expected: exit 0, no output (valid syntax). Make it executable: `chmod +x tests/performance/profile/run_pyspy.sh`.

- [ ] **Step 5: Commit**

```bash
git add docker-compose.profiling.yml tests/performance/profile/run_pyspy.sh
git commit -m "feat: profiling compose override and py-spy Layer 2 helper

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 7: Documentation — runbook and CLAUDE.md

**Files:**
- Create: `docs/superpowers/perf-profiling-runbook.md`
- Modify: `CLAUDE.md`

- [ ] **Step 1: Write the runbook**

Create `docs/superpowers/perf-profiling-runbook.md`:

```markdown
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
```

- [ ] **Step 2: Update CLAUDE.md**

In `python-webservice-template/CLAUDE.md`, add this subsection under **Key Patterns**, immediately after the **Performance Tests** block:

```markdown
**Performance Profiling**
- Two-layer bottleneck triage; see `docs/superpowers/perf-profiling-runbook.md`.
- Layer 1 (every run, report-only): `core/boundary_timing.py` emits a W3C `Server-Timing` response header built from `timed()` boundary samples held in a request-scoped `ContextVar`; `tests/performance/profile_reads.js` and `profile_ingest.js` parse it into a ranked per-endpoint attribution table + `attribution.json`.
- Layer 2 (on demand): `tests/performance/profile/run_pyspy.sh` attaches py-spy to the app container (needs `docker-compose.profiling.yml` for the `SYS_PTRACE` cap) and emits flamegraphs that classify CPU/GIL- vs I/O-bound contention.
- Boundary samples are request-scoped, so background work (the streaming ingest thread) never pollutes a request's header and multiple isolated test apps cannot collide.
```

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/perf-profiling-runbook.md CLAUDE.md
git commit -m "docs: performance profiling runbook + CLAUDE.md tooling section

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-Review

**1. Spec coverage:**
- Layer 1 report-only → Tasks 1-5 (no thresholds fail the build; `summarize` is report-only). ✓
- `Server-Timing` via request-scoped ContextVar → Tasks 1-3. ✓
- `timed()` reuse, no constructor changes → Task 2, Task 4. ✓
- Seven boundaries → existing four + Task 4's three. ✓
- Scope reads + ingest write path → Task 5 read/ingest profiles. ✓
- Ingest isolation (idle stream, sole writer) → Task 6 override + Task 5 ingest profile note. ✓
- Layer 2 scripted helper + runbook → Tasks 6-7. ✓
- Decision rule (CPU/GIL vs I/O) → Task 7 runbook table. ✓
- Testing: real endpoints/containers, isolation test → Tasks 3-4 (`tests/test_server_timing.py`). ✓
- Error handling: header never breaks response; parser tolerant; record_boundary no-op off-request → Task 1 (`try/except` in `send_wrapper`, `parseServerTiming` guards, None-list no-op). ✓

**2. Placeholder scan:** No TBD/TODO; every code step shows complete content; commands have expected output. ✓

**3. Type/name consistency:** `boundary_samples_var`, `record_boundary`, `_render_header`, `ServerTimingMiddleware`, `recordServerTiming(res, endpoint)`, `summarize(data)`, endpoint keys `data`/`cache`/`ingest`, boundary tokens (`clickhouse_select`, `lsm_query`, `ingest_decode`, `ingest_lsm_write`) are used identically across server, tests, and k6. The `ENDPOINTS` label lists match the tokens the middleware emits (dots → underscores). ✓
```
