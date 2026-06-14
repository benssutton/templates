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
