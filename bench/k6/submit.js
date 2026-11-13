// k6 load test for the REST API: single-job submissions at a constant arrival rate while
// the cluster runs them. Run with scripts/k6.sh (on the compose network).
import http from 'k6/http';
import { check } from 'k6';

const BASE = __ENV.GOPHERGRID_URL || 'http://scheduler-1:8080';
const RATE = Number(__ENV.RATE || 500);

export const options = {
  scenarios: {
    submit: {
      executor: 'constant-arrival-rate',
      rate: RATE, timeUnit: '1s', duration: __ENV.DURATION || '30s',
      preAllocatedVUs: 50, maxVUs: 400,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<100'],
  },
};

const body = JSON.stringify({ type: 'sleep', payload: { ms: 50 }, resources: { cpu_millis: 100, memory_mb: 16 } });
const params = { headers: { 'Content-Type': 'application/json' } };

export default function () {
  const res = http.post(`${BASE}/v1/jobs`, body, params);
  check(res, { 'created': (r) => r.status === 201 });
}

export function handleSummary(data) {
  const d = data.metrics.http_req_duration.values;
  const out = {
    name: 'k6-submit', rate_target: RATE,
    requests: data.metrics.http_reqs.values.count,
    rps: data.metrics.http_reqs.values.rate,
    failed_rate: data.metrics.http_req_failed.values.rate,
    latency_ms: { p50: d.med, p90: d['p(90)'], p95: d['p(95)'], max: d.max },
    thresholds_ok: Object.values(data.metrics).every((m) => !m.thresholds || Object.values(m.thresholds).every((t) => t.ok)),
  };
  return { stdout: JSON.stringify(out, null, 2) + '\n', '/out/k6-submit.json': JSON.stringify(out, null, 2) + '\n' };
}
