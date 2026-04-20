# Load & Chaos Testing

## Load Test

Requires [k6](https://k6.io/docs/get-started/installation/) (`brew install k6`).

```bash
# Start the stack + ingest API
make up
go run ./cmd/ingest-api &

# Run the load test (default: ramp 50 → 1000 VUs over 5 minutes)
k6 run loadtest/ingest.js

# Override target URL
K6_INGEST_URL=http://localhost:8080/events k6 run loadtest/ingest.js

# Quick smoke (lower load)
k6 run --vus 10 --duration 30s loadtest/ingest.js
```

### Thresholds

| Metric | Target |
|--------|--------|
| p95 latency | < 200ms |
| p99 latency | < 500ms |
| Error rate | < 1% |

### What it tests

- Ingestion API throughput under sustained load
- Kafka producer backpressure behavior
- Batch processing efficiency (10 events per request)
- Connection pool saturation

## Chaos Scenarios

Manual scenarios to run against the running stack. After each scenario,
verify recovery by checking that Postgres state matches expected counts
(no data loss, no extra duplicates in raw_events).

### 1. Kill stream-processor mid-load

```bash
# Start load
k6 run --vus 50 --duration 2m loadtest/ingest.js &

# Kill the processor after 30s
sleep 30 && kill $(pgrep -f stream-processor)

# Restart it
go run ./cmd/stream-processor &

# After load completes, verify:
# - Consumer group lag drops to 0
# - raw_events count matches expected (no loss)
# - entity_positions are correct (no stale overwrites)
```

### 2. Kill Kafka broker

```bash
docker stop dm-kafka
sleep 10
docker start dm-kafka

# Verify: ingest API returns errors during outage, recovers after broker restarts.
# Consumer resumes from last committed offset.
```

### 3. Postgres connection storm

```bash
# Run load with many VUs to saturate the pgx pool
k6 run --vus 500 --duration 1m loadtest/ingest.js

# Monitor: pg pool saturation, query latency, connection wait time.
# Pool limits (MaxConns=10 for processor, 20 for query) should bound connections.
```

### 4. Consumer rebalance under load

```bash
# Start two processor instances
go run ./cmd/stream-processor &
KAFKA_GROUP_ID=mobility-postgres go run ./cmd/stream-processor &

# Start load, then kill one instance
k6 run --vus 100 --duration 2m loadtest/ingest.js &
sleep 30 && kill $(pgrep -f stream-processor | head -1)

# Verify: rebalance completes, no data loss, lag recovers.
```

## Verification After Chaos

```sql
-- Expected: unique event count matches generator output
SELECT count(*) FROM raw_events;

-- No duplicate event_ids
SELECT event_id, count(*) FROM raw_events GROUP BY event_id HAVING count(*) > 1;

-- Entity positions reflect the latest timestamp per entity
SELECT entity_type, entity_id, event_ts, last_event_id
FROM entity_positions
ORDER BY entity_type, entity_id;
```
