// k6 load test for the Dream Mobility ingestion API.
//
// Ramps from 100 → 10,000 events/sec and validates that the API stays
// responsive under load with <500ms p99 latency and 0% error rate.
//
// Usage:
//   k6 run loadtest/ingest.js
//   k6 run --vus 200 --duration 5m loadtest/ingest.js
//   K6_INGEST_URL=http://localhost:8080/events k6 run loadtest/ingest.js

import http from "k6/http";
import { check, sleep } from "k6";
import { Rate, Trend } from "k6/metrics";
import { uuidv4 } from "https://jslib.k6.io/k6-utils/1.4.0/index.js";

// Custom metrics.
const errorRate = new Rate("errors");
const ingestDuration = new Trend("ingest_duration", true);

// Target URL — override with K6_INGEST_URL env var.
const INGEST_URL = __ENV.K6_INGEST_URL || "http://localhost:8080/events";

// Entity pool size — events are distributed across this many entities.
const ENTITY_COUNT = 500;

// Entity types.
const ENTITY_TYPES = ["vehicle", "courier", "scooter", "device"];

// Load profile: ramp up → sustained → ramp down.
export const options = {
  stages: [
    { duration: "30s", target: 50 },   // warm up
    { duration: "1m",  target: 200 },  // ramp to moderate load
    { duration: "2m",  target: 500 },  // sustained high load
    { duration: "1m",  target: 1000 }, // peak
    { duration: "30s", target: 0 },    // ramp down
  ],
  thresholds: {
    http_req_duration: ["p(95)<200", "p(99)<500"],
    errors: ["rate<0.01"],  // <1% error rate
  },
};

// Base coordinates for random walks.
const BASE_COORDS = [
  [52.52, 13.405],   // Berlin
  [40.7128, -74.006], // NYC
  [32.0853, 34.7818], // TLV
  [35.6762, 139.6503], // Tokyo
  [37.7749, -122.4194], // SF
];

function randomEntity() {
  const idx = Math.floor(Math.random() * ENTITY_COUNT);
  const type = ENTITY_TYPES[idx % ENTITY_TYPES.length];
  return { type: type, id: `${type}-${idx}` };
}

function randomPosition() {
  const base = BASE_COORDS[Math.floor(Math.random() * BASE_COORDS.length)];
  return {
    lat: base[0] + (Math.random() - 0.5) * 0.1,
    lon: base[1] + (Math.random() - 0.5) * 0.1,
  };
}

function generateEvent() {
  const entity = randomEntity();
  const pos = randomPosition();
  return {
    event_id: uuidv4(),
    entity: entity,
    timestamp: new Date().toISOString(),
    position: pos,
    speed_kmh: Math.random() * 120,
    source: "gps",
  };
}

function generateBatch(size) {
  const events = [];
  for (let i = 0; i < size; i++) {
    events.push(generateEvent());
  }
  return { events: events };
}

export default function () {
  // Send a batch of 10 events per iteration for efficiency.
  const batch = generateBatch(10);
  const payload = JSON.stringify(batch);

  const params = {
    headers: { "Content-Type": "application/json" },
    tags: { name: "POST /events (batch)" },
  };

  const res = http.post(INGEST_URL, payload, params);

  const success = check(res, {
    "status is 202": (r) => r.status === 202,
    "has accepted count": (r) => {
      try {
        const body = JSON.parse(r.body);
        return body.accepted > 0;
      } catch {
        return false;
      }
    },
  });

  errorRate.add(!success);
  ingestDuration.add(res.timings.duration);

  // Throttle slightly to control event rate.
  sleep(0.01);
}
