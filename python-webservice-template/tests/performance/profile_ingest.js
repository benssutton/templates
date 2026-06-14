import http from 'k6/http';
import { check } from 'k6';
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
  check(res, { 'POST /data/ingest is 202': (r) => r.status === 202 });
  recordServerTiming(res, 'ingest');
}

export function handleSummary(data) {
  return summarize(data);
}
