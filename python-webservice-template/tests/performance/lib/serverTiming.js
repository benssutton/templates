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

// "clickhouse_select;dur=12.30, total;dur=15.00" -> { clickhouse_select: 12.3, total: 15 }
// Finds the `dur` parameter anywhere in the entry, so an optional desc=… (the
// fuller W3C Server-Timing grammar) does not drop the sample. Matches the
// Python _render_header output and tolerates richer headers.
export function parseServerTiming(header) {
  const out = {};
  if (!header) return out;
  for (const part of header.split(',')) {
    const nameM = part.trim().match(/^([A-Za-z0-9_]+)/);
    const durM = part.match(/(?:^|;)\s*dur=([0-9.]+)/);
    if (nameM && durM) out[nameM[1]] = parseFloat(durM[1]);
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
  // p95_ms is null: the p95 of a difference (total - sum of boundaries) is not a meaningful statistic.
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
