# CLAUDE.md — operational notes for Claude sessions

Architecture, invariants, make targets, and the full data model are in
[README.md](./README.md). Don't duplicate them here. This file is only the
things that are *not* obvious from the README and that a future session
would trip over.

## Commit convention

- Personal learning project. **Do not** use the global `CM-XXXXX:` Jira
  prefix on commits in this repo. Plain Conventional Commits
  (`feat:`, `fix:`, `docs:`, ...) is the style — see `git log`.

## Running services locally

- The four Go services each bind a distinct Prometheus port via
  `PROM_PORT` (defaults: `9464` ingest / `9465` query / `9466` processor
  / `9467` ch-sink). **Running multiple replicas of the same service on
  the host requires passing distinct `PROM_PORT` values** or the second
  instance fails with `bind: address already in use`.
- `go run` spawns both a wrapper process and the built binary. Killing
  the wrapper pid does not kill the binary — find the real pid via
  `lsof -iTCP -sTCP:LISTEN -P | grep 94..`.

## Python tools need S3 creds explicitly

- `services/archiver/archiver.py` and `tools/lake-query/query.py`
  **refuse to start without** `S3_ACCESS_KEY` / `S3_SECRET_KEY`. There
  is no local default on purpose (guards against a fat-fingered
  invocation reaching a non-local lake with `minioadmin`). For local
  dev: `S3_ACCESS_KEY=minioadmin S3_SECRET_KEY=minioadmin`.

## Kafka partitioning dictates scale-out

- Topic `movement.events` has **3 partitions**. Per consumer group, at
  most 3 instances consume in parallel; a 4th sits idle with
  `#PARTITIONS = 0` (verify with
  `kafka-consumer-groups --describe --group <g> --members`).
- The three sinks (`mobility-postgres`, `mobility-clickhouse`,
  `mobility-iceberg`) are **independent consumer groups** and scale
  independently.
- Per-entity ordering (invariant I6) holds under scale-out because the
  producer keys on `entity_type:entity_id` — same key → same partition
  → single consumer.

## Observability stack caveats

- Opt-in overlay compose:
  `docker compose -f deploy/docker-compose.yml
  -f deploy/observability/docker-compose.observability.yml -p dream-mobility up -d`.
- OTel SDK is initialized in every `cmd/*/main.go`. The OTLP/HTTP
  exporter **must** use `otlptracehttp.WithInsecure()` against the
  local collector (the SDK defaults to HTTPS otherwise and every
  export silently fails with `server gave HTTP response to HTTPS
  client`).
- **Custom metrics**: still none. `/metrics` is Go runtime + process
  only. Adding ingest/sink counters is deferred work.
- **Traces**: the `POST /events` → Kafka → stream-processor →
  Postgres path is instrumented end-to-end. W3C TraceContext rides
  on Kafka message headers via the carrier in
  `internal/tracing/kafka.go`. clickhouse-sink and archiver are not
  yet instrumented.
- Prometheus is reachable from Grafana at
  `http://dm-prometheus:9090` (the docker-network hostname), not
  `localhost`. Jaeger is reachable the same way at
  `http://dm-jaeger:16686`. Datasources are provisioned by hand
  today (`curl -u admin:admin -X POST /api/datasources`).
- Grafana runs with anonymous Viewer + `admin/admin` for edit. Dev-only.
- `deploy/observability/prometheus.yml` statically scrapes three
  `stream-processor` ports (9466/9476/9486) so three local replicas
  are visible. This is a local-dev hack — in k8s swap for a
  `PodMonitor` label selector.
