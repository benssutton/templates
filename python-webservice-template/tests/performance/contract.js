// The HTTP contract shared by the Python, Java and Rust templates.
//
// This file is identical in all three services (root tests/cicd/test_perf_scripts.py)
// and runs in the k6-smoke step against each service's compose stack, so the three
// cannot drift apart without that step failing. It pins what a client may rely on:
// paths, status codes, snake_case field names, the {"detail"} error body, ingest
// media types, the merged /data/cache view, the MCP tools and their REST-identical
// results, the shared metric names and the X-Request-ID rule. Env var names are
// pinned by tests/cicd/test_env_contract.py. Service-specific extras (for example
// stream-store statistics under /health/status ingest) are allowed.
//
// 401 (auth is off by default), 500 and 503 need a configured or broken stack,
// so each service pins those in its own test suite.
import http from 'k6/http';
import { check, group, sleep } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'https://localhost';
const BATCH = open('./data/ingest_batch.ipc', 'b');
const CONTRACT_BATCH = open('./data/contract_batch.ipc', 'b');
const ARROW = 'application/vnd.apache.arrow.stream';
const JSON_HEADERS = { 'Content-Type': 'application/json' };

export const options = {
  vus: 1,
  iterations: 1,
  thresholds: { checks: ['rate==1.0'] },
};

function body(r) {
  try { return JSON.parse(r.body); } catch { return undefined; }
}

function keys(obj) {
  return obj && typeof obj === 'object' ? Object.keys(obj).sort() : [];
}

function hasKeys(obj, expected) {
  const actual = keys(obj);
  return expected.every((k) => actual.includes(k));
}

function sameKeys(obj, expected) {
  return JSON.stringify(keys(obj)) === JSON.stringify([...expected].sort());
}

// Object keys sorted at every level, so deep equality is a string compare.
function canon(v) {
  if (Array.isArray(v)) return v.map(canon);
  if (v && typeof v === 'object') {
    return Object.keys(v).sort().reduce((o, k) => { o[k] = canon(v[k]); return o; }, {});
  }
  return v;
}

function same(a, b) {
  return a !== undefined && JSON.stringify(canon(a)) === JSON.stringify(canon(b));
}

// Ingest may be written after its 202, so reads poll briefly.
function eventually(fn, seconds) {
  const deadline = Date.now() + seconds * 1000;
  let result = fn();
  while (!result && Date.now() < deadline) {
    sleep(0.2);
    result = fn();
  }
  return result;
}

// Every error: the given status, application/json, {"detail": <string or list>}.
function checkError(name, r, status) {
  check(r, {
    [`${name}: status ${status}`]: (res) => res.status === status,
    [`${name}: application/json`]: (res) =>
      (res.headers['Content-Type'] || '').startsWith('application/json'),
    [`${name}: {"detail"} body`]: (res) => {
      const b = body(res);
      return sameKeys(b, ['detail']) &&
        (typeof b.detail === 'string' || Array.isArray(b.detail));
    },
  });
}

const MCP_TOOLS = ['get_cache', 'get_cached_data', 'get_config', 'get_data', 'get_health_status'];

// A JSON-RPC response arrives as JSON or as an SSE stream; take its last data line.
function rpcBody(r) {
  if ((r.headers['Content-Type'] || '').includes('text/event-stream')) {
    const lines = (r.body || '').split('\n').filter((l) => l.startsWith('data:'));
    return lines.length ? JSON.parse(lines[lines.length - 1].slice(5)) : undefined;
  }
  return body(r);
}

function mcpSession() {
  const headers = { ...JSON_HEADERS, Accept: 'application/json, text/event-stream' };
  let id = 0;
  const post = (msg) => http.post(`${BASE_URL}/mcp`, JSON.stringify(msg), { headers });
  const init = post({
    jsonrpc: '2.0', id: ++id, method: 'initialize',
    params: { protocolVersion: '2025-03-26', capabilities: {}, clientInfo: { name: 'contract', version: '1' } },
  });
  const session = init.headers['Mcp-Session-Id'];
  if (session) headers['Mcp-Session-Id'] = session;
  post({ jsonrpc: '2.0', method: 'notifications/initialized' });
  return {
    initialized: init.status === 200,
    request: (method, params) => rpcBody(post({ jsonrpc: '2.0', id: ++id, method, params })),
  };
}

// A tool result must be a single text item holding the REST body as JSON.
function toolJson(result) {
  const content = (result && result.content) || [];
  if (content.length !== 1 || content[0].type !== 'text') return undefined;
  try { return JSON.parse(content[0].text); } catch { return undefined; }
}

export default function () {
  const run = `${Date.now()}-${Math.floor(Math.random() * 1e6)}`;

  group('root', () => {
    const r = http.get(`${BASE_URL}/`);
    check(r, {
      'root: 200': (res) => res.status === 200,
      'root: keys': (res) =>
        sameKeys(body(res), ['title', 'version', 'description', 'docs', 'MCP']),
      'root: MCP is /mcp': (res) => (body(res) || {}).MCP === '/mcp',
    });
  });

  group('health', () => {
    const live = http.get(`${BASE_URL}/health/live`);
    check(live, {
      'live: 200': (res) => res.status === 200,
      'live: keys': (res) => sameKeys(body(res), ['status', 'uptime_seconds']),
    });

    const ready = http.get(`${BASE_URL}/health/ready`);
    check(ready, {
      'ready: 200': (res) => res.status === 200,
      'ready: status ready': (res) => (body(res) || {}).status === 'ready',
      'ready: checks have name and status': (res) => {
        const checks = (body(res) || {}).checks;
        return Array.isArray(checks) && checks.length > 0 &&
          checks.every((c) => hasKeys(c, ['name', 'status']));
      },
    });

    const status = http.get(`${BASE_URL}/health/status`);
    const s = body(status) || {};
    check(status, {
      'status: 200': (res) => res.status === 200,
      'status: top-level keys': () =>
        sameKeys(s, ['app', 'uptime', 'dependencies', 'ingest', 'requests', 'system']),
      'status: app keys': () => sameKeys(s.app, ['title', 'version', 'status']),
      'status: uptime keys': () =>
        sameKeys(s.uptime, ['process_seconds', 'system_boot_seconds']),
      'status: dependencies are postgres, clickhouse, redis': () =>
        Array.isArray(s.dependencies) &&
        JSON.stringify(s.dependencies.map((d) => d.name).sort()) ===
          JSON.stringify(['clickhouse', 'postgres', 'redis']),
      'status: dependency keys': () =>
        Array.isArray(s.dependencies) &&
        s.dependencies.every((d) => sameKeys(d, ['name', 'status', 'latency_ms', 'error'])),
      'status: ingest keys': () =>
        hasKeys(s.ingest, [
          'transport', 'connection_state', 'thread_alive', 'last_batch_at',
          'seconds_since_last_batch', 'rows_ingested_total', 'stale',
        ]),
      'status: requests keys': () => sameKeys(s.requests, ['last_request_at']),
      'status: system keys': () =>
        sameKeys(s.system, ['process', 'host']) &&
        sameKeys(s.system.process,
          ['cpu_percent', 'memory_rss_bytes', 'num_threads', 'open_files']) &&
        sameKeys(s.system.host,
          ['cpu_percent', 'memory_total_bytes', 'memory_available_bytes', 'memory_percent']),
    });
  });

  group('config', () => {
    const key = `contract-config-${run}`;
    const created = http.post(`${BASE_URL}/config`,
      JSON.stringify({ key, value: 'v' }), { headers: JSON_HEADERS });
    check(created, {
      'config create: 201': (res) => res.status === 201,
      'config create: body': (res) =>
        JSON.stringify(body(res)) === JSON.stringify({ key, value: 'v' }),
    });
    const list = http.get(`${BASE_URL}/config`);
    check(list, {
      'config list: 200': (res) => res.status === 200,
      'config list: contains the entry': (res) => {
        const b = body(res);
        return Array.isArray(b) && b.some((e) => sameKeys(e, ['key', 'value']) && e.key === key);
      },
    });
  });

  group('cache', () => {
    const withTtl = `contract-cache-ttl-${run}`;
    const noTtl = `contract-cache-${run}`;
    const created = http.post(`${BASE_URL}/cache`,
      JSON.stringify({ key: withTtl, value: { a: 1 }, ttl_seconds: 60 }), { headers: JSON_HEADERS });
    check(created, {
      'cache create: 201': (res) => res.status === 201,
      'cache create: body': (res) =>
        JSON.stringify(body(res)) ===
          JSON.stringify({ key: withTtl, value: { a: 1 }, ttl_seconds: 60 }),
    });
    http.post(`${BASE_URL}/cache`,
      JSON.stringify({ key: noTtl, value: 's' }), { headers: JSON_HEADERS });
    check(http.get(`${BASE_URL}/cache/${noTtl}`), {
      'cache get: ttl_seconds null without a TTL': (res) =>
        res.status === 200 &&
        JSON.stringify(body(res)) === JSON.stringify({ key: noTtl, value: 's', ttl_seconds: null }),
    });
    check(http.get(`${BASE_URL}/cache/${withTtl}`), {
      'cache get: remaining TTL': (res) => {
        const b = body(res) || {};
        return res.status === 200 && sameKeys(b, ['key', 'value', 'ttl_seconds']) &&
          b.ttl_seconds > 0 && b.ttl_seconds <= 60;
      },
    });
  });

  group('data', () => {
    const r = http.get(`${BASE_URL}/data?limit=5`);
    const b = body(r) || {};
    check(r, {
      'data: 200': (res) => res.status === 200,
      'data: keys': () => sameKeys(b, ['rows', 'total', 'limit']),
      'data: ordered by id': () =>
        Array.isArray(b.rows) && b.rows.every((row, i) => i === 0 || b.rows[i - 1].id <= row.id),
    });

    for (const type of [ARROW, 'application/octet-stream']) {
      check(http.post(`${BASE_URL}/data/ingest`, BATCH, { headers: { 'Content-Type': type } }), {
        [`ingest (${type}): 202 {"accepted": true}`]: (res) =>
          res.status === 202 && JSON.stringify(body(res)) === JSON.stringify({ accepted: true }),
      });
    }
  });

  group('data cache', () => {
    check(http.post(`${BASE_URL}/data/ingest`, CONTRACT_BATCH, { headers: { 'Content-Type': ARROW } }), {
      'data cache: contract batch accepted': (res) => res.status === 202,
    });
    const expected = [
      { id: -3, name: 'contract', value: 'v2' },
      { id: -1, name: 'contract', value: 'v1' },
    ];
    let b = {};
    const merged = eventually(() => {
      const r = http.get(`${BASE_URL}/data/cache?limit=100`);
      b = body(r) || {};
      return r.status === 200 && Array.isArray(b.rows) &&
        same(b.rows.filter((row) => row.id < 0), expected);
    }, 5);
    check(b, {
      'data cache: keys': () => sameKeys(b, ['rows', 'total', 'limit']),
      'data cache: limit echoed': () => b.limit === 100,
      'data cache: rows are exactly id, name, value': () =>
        Array.isArray(b.rows) && b.rows.every((row) => sameKeys(row, ['id', 'name', 'value'])),
      'data cache: one row per key, ascending': () =>
        Array.isArray(b.rows) && b.rows.every((row, i) => i === 0 || b.rows[i - 1].id < row.id),
      'data cache: newest version wins, tombstones removed': () => merged,
      'data cache: total counts live rows': () => typeof b.total === 'number' && b.total >= b.rows.length,
    });
  });

  group('mcp', () => {
    const mcp = mcpSession();
    const call = (name, args) => (mcp.request('tools/call', { name, arguments: args }) || {}).result;
    const rest = (path) => body(http.get(`${BASE_URL}${path}`));

    const listed = mcp.request('tools/list', {});
    const names = ((listed && listed.result && listed.result.tools) || []).map((t) => t.name).sort();

    const key = `contract-mcp-${run}`;
    http.post(`${BASE_URL}/cache`, JSON.stringify({ key, value: { n: 7 } }), { headers: JSON_HEADERS });
    const missingKey = `contract-mcp-missing-${run}`;
    const missing = call('get_cache', { key: missingKey });

    check(null, {
      'mcp: initialize 200': () => mcp.initialized,
      'mcp: tool names': () => same(names, MCP_TOOLS),
      'mcp: get_config = GET /config': () => same(toolJson(call('get_config', {})), rest('/config')),
      'mcp: get_cache = GET /cache/{key}': () =>
        same(toolJson(call('get_cache', { key })), rest(`/cache/${key}`)),
      'mcp: get_cache missing = isError + the 404 body': () =>
        !!missing && missing.isError === true && same(toolJson(missing), rest(`/cache/${missingKey}`)),
      'mcp: get_data = GET /data': () =>
        same(toolJson(call('get_data', { limit: 5 })), rest('/data?limit=5')),
      'mcp: get_cached_data has the /data/cache keys': () =>
        sameKeys(toolJson(call('get_cached_data', { limit: 5 })), ['rows', 'total', 'limit']),
      'mcp: get_health_status has the /health/status keys': () =>
        sameKeys(toolJson(call('get_health_status', {})), keys(rest('/health/status'))),
    });
  });

  group('errors', () => {
    checkError('unknown route', http.get(`${BASE_URL}/no-such-route`), 404);
    const missing = http.get(`${BASE_URL}/cache/contract-missing-${run}`);
    checkError('missing cache key', missing, 404);
    checkError('wrong method', http.del(`${BASE_URL}/config`), 405);

    checkError('malformed JSON',
      http.post(`${BASE_URL}/config`, '{not json', { headers: JSON_HEADERS }), 400);
    checkError('unreadable Arrow',
      http.post(`${BASE_URL}/data/ingest`, 'not arrow', { headers: { 'Content-Type': ARROW } }), 400);

    checkError('JSON route, text/plain',
      http.post(`${BASE_URL}/config`, '{"key":"k","value":"v"}',
        { headers: { 'Content-Type': 'text/plain' } }), 415);
    checkError('ingest, text/plain',
      http.post(`${BASE_URL}/data/ingest`, BATCH, { headers: { 'Content-Type': 'text/plain' } }), 415);

    checkError('empty cache key',
      http.post(`${BASE_URL}/cache`, '{"key":"","value":1}', { headers: JSON_HEADERS }), 422);
    checkError('zero TTL',
      http.post(`${BASE_URL}/cache`, '{"key":"k","value":1,"ttl_seconds":0}', { headers: JSON_HEADERS }), 422);
    checkError('limit below range', http.get(`${BASE_URL}/data?limit=0`), 422);
    checkError('limit not a number', http.get(`${BASE_URL}/data?limit=abc`), 422);

    // Over the 16 MiB default max_request_body_bytes.
    const tooBig = new Uint8Array(17 * 1024 * 1024).buffer;
    checkError('body over the limit',
      http.post(`${BASE_URL}/data/ingest`, tooBig, { headers: { 'Content-Type': ARROW } }), 413);

    checkError('MCP foreign Host',
      http.post(`${BASE_URL}/mcp/`, '{"jsonrpc":"2.0","id":1,"method":"ping"}', {
        headers: {
          ...JSON_HEADERS,
          Accept: 'application/json, text/event-stream',
          Host: 'evil.example',
        },
        redirects: 0,
      }), 403);
  });

  group('request id', () => {
    const valid = 'contract-id.1_2';
    check(http.get(`${BASE_URL}/health/live`, { headers: { 'X-Request-ID': valid } }), {
      'request id: a valid id is echoed': (res) => res.headers['X-Request-Id'] === valid,
    });
    const invalid = 'not valid!';
    check(http.get(`${BASE_URL}/health/live`, { headers: { 'X-Request-ID': invalid } }), {
      'request id: an invalid id is replaced': (res) => {
        const id = res.headers['X-Request-Id'];
        return typeof id === 'string' && id.length > 0 && id !== invalid;
      },
    });
  });
}
