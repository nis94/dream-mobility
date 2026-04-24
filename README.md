# Dream Mobility — Real-Time Movement Intelligence

A backend pipeline that ingests high-volume GPS-like movement events, validates
and normalizes them, and stores both raw and derived representations so that
downstream applications can query last-known position, time-series scans, and
long-term analytics.

Built as a personal learning project around the Dream Security Detection-group
take-home assignment. The same task is the vehicle for exercising the full
production stack: Go, Python, Kafka, Confluent Schema Registry + Avro, Postgres,
ClickHouse, S3 (MinIO), Apache Iceberg + Parquet, Kubernetes + Helm,
OpenTelemetry.

## Capabilities

| Capability | Path | Latency target |
|---|---|---|
| Accept a movement event | HTTP `POST /events` on ingest-api | <50 ms p99 |
| Deliver to every downstream store | Kafka → independent consumer groups | seconds |
| Last-known position for an entity | HTTP `GET /entities/{type}/{id}/position` | <20 ms p99 (point read) |
| Reverse-chronological event history | HTTP `GET /entities/{type}/{id}/events?limit=&cursor=` | <50 ms p99 |
| Dashboard-grade hourly rollups | ClickHouse `events_hourly_final` view | sub-second scans |
| Long-term analytical replay | Iceberg + DuckDB / Trino / Spark | minutes |

The pipeline is deliberately **three-store** — Postgres for hot state,
ClickHouse for dashboards, Iceberg for the lake — because each answers a
different question better than the other two. Kafka is the fan-out point.

## Architecture

```mermaid
flowchart LR
    EXT[External Clients] -->|HTTP POST /events| API[ingest-api<br/>Go + chi]
    GEN[Python Synthetic<br/>Generator] -->|HTTP| API
    API -->|Avro + SR wire format| KAFKA[(Kafka topic<br/>movement.events<br/>key = entity_type:entity_id)]
    SR[(Schema Registry)] -.->|schema id cache| API

    KAFKA --> PROC[stream-processor<br/>group: mobility-postgres]
    KAFKA --> CHSINK[clickhouse-sink<br/>group: mobility-clickhouse<br/>batched, per-batch commit]
    KAFKA --> ARCH[archiver<br/>group: mobility-iceberg<br/>PyIceberg]

    PROC -->|ON CONFLICT + tx| PG[(Postgres<br/>raw_events +<br/>entity_positions)]
    CHSINK -->|batched INSERT| CH[(ClickHouse<br/>ReplacingMergeTree +<br/>AggregatingMergeTree MV)]
    ARCH -->|Parquet| MINIO[(MinIO / S3)]
    ARCH -.->|metadata| ICAT[Iceberg REST Catalog]

    PG --> QAPI[query-api<br/>Go]
    QAPI --> CLI[curl / clients]
    MINIO --> LAKE[lake-query CLI<br/>DuckDB]

    API -.-> OTEL[OTel Collector]
    PROC -.-> OTEL
    CHSINK -.-> OTEL
    QAPI -.-> OTEL
    OTEL -.-> PROM[Prometheus + Grafana]
```

All three sinks work off the same topic via **distinct consumer groups**, so
each has its own committed offset and a replay on one sink does not affect
the others.

## End-to-end data flow

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant A as ingest-api
    participant K as Kafka<br/>movement.events
    participant P as stream-processor
    participant H as clickhouse-sink
    participant I as archiver
    participant PG as Postgres
    participant CH as ClickHouse
    participant L as Iceberg/MinIO
    participant Q as query-api

    C->>A: POST /events {batch or single JSON}
    A->>A: validate → map to flat Avro record
    A->>A: Marshal + prepend SR wire format<br/>(0x00 + schema id + Avro binary)
    A->>K: WriteMessages key=entity_type:entity_id (Murmur2)
    A-->>C: 202 {accepted, rejected, errors[]}

    par independent consumer groups
        K->>P: FetchMessage(mobility-postgres)
        P->>P: decode SR wire format
        P->>PG: BEGIN; INSERT raw_events ON CONFLICT; UPSERT entity_positions (timestamp-gated)
        PG-->>P: committed
        P->>K: CommitMessages (detached 5s ctx)
    and
        K->>H: FetchMessage(mobility-clickhouse)
        H->>H: buffer up to batch_size or batch_timeout
        H->>CH: prepareBatch / Append / Send
        CH-->>H: ok
        H->>K: CommitMessages for the whole batch
    and
        K->>I: poll(mobility-iceberg)
        I->>I: buffer + Arrow RecordBatch
        I->>L: table.append(parquet)
        I->>K: commit offsets
    end

    Note over C,Q: read path
    C->>Q: GET /entities/vehicle/v7/position
    Q->>PG: point read entity_positions
    Q-->>C: 200 {lat, lon, event_ts, last_event_id}
    C->>Q: GET /entities/vehicle/v7/events?limit=50
    Q->>PG: row-wise keyset before cursor (event_ts, event_id)
    Q-->>C: 200 {events[], next_cursor}
```

The key invariant running through every arrow: **the Kafka offset advances
only after the downstream store has durably accepted the write.** A crash
between DB commit and offset commit replays the last message — and the
sink-side idempotence keys (`event_id` in Postgres / ClickHouse / read-time
GROUP BY in Iceberg) make that replay a no-op.

## Correctness invariants

| # | Invariant | Enforced by |
|---|---|---|
| I1 | Every event has a unique `event_id` post-dedupe in Postgres | `raw_events.event_id PRIMARY KEY` + `ON CONFLICT DO NOTHING` |
| I2 | `entity_positions.event_ts` equals `MAX(raw_events.event_ts)` per entity | `WHERE EXCLUDED.event_ts > entity_positions.event_ts` in the upsert |
| I3 | Kafka offset for a message advances only after its downstream write commits | `FetchMessage` → write → `CommitMessages`, with detached context for the commit so SIGTERM doesn't leave the last offset behind |
| I4 | ClickHouse `event_count` per entity/hour (from `events_hourly_final`) equals distinct `event_id` count in the matching raw time range | `AggregatingMergeTree` with `uniqExactState(event_id)` + read-side `uniqExactMerge` |
| I5 | Paginated events: every `event_id` is served at most once across pages, including events sharing the same microsecond | Composite `(event_ts, event_id)` cursor + row-wise `< (ts, id)` SQL + `ORDER BY event_ts DESC, event_id DESC` |
| I6 | Per-entity ordering is preserved into each sink | Producer Murmur2 key = `entity_type:entity_id` → same partition → single consumer owns the partition → sequential reads |

Integration tests (`internal/processor/store_test.go`, build tag
`integration`) verify I1/I2 against a `testcontainers-go` Postgres. Unit
tests verify I5 (`TestListEvents_TieBreakAcrossPages`). The end-to-end
smoke below verifies all six together on a live stack.

## Data model

### Avro wire contract (Kafka + Schema Registry)

`internal/avro/movement_event.avsc`

```text
record MovementEvent {
    event_id:    string  (logicalType=uuid),       // dedupe key across every sink
    entity_type: string,
    entity_id:   string,
    timestamp:   long    (logicalType=timestamp-micros),
    lat, lon:    double,
    speed_kmh, heading_deg, accuracy_m: [null, double],
    source:      [null, string],
    attributes:  [null, string]                    // opaque JSON
}
```

Wire format on the topic: `0x00` magic + big-endian uint32 schema ID + Avro
binary payload.

### Postgres — hot state (`internal/processor/schema.sql`)

```sql
raw_events (
    event_id    UUID PRIMARY KEY,
    entity_type TEXT, entity_id TEXT,
    event_ts    TIMESTAMPTZ,
    lat, lon, speed_kmh, heading_deg, accuracy_m, source, attributes, ingested_at,
    CHECK (lat BETWEEN -90 AND 90), CHECK (lon BETWEEN -180 AND 180)
);
INDEX idx_raw_events_entity_ts (entity_type, entity_id, event_ts DESC);

entity_positions (
    PRIMARY KEY (entity_type, entity_id),
    event_ts, lat, lon, last_event_id, updated_at
);
-- Upsert: WHERE EXCLUDED.event_ts > entity_positions.event_ts  (strict >)
```

Strict `>` on the position upsert discards stale events; `READ COMMITTED` +
the `WHERE` guard are sufficient under a single consumer.

### ClickHouse — analytics (`internal/chsink/schema.sql`)

```sql
raw_events ReplacingMergeTree(event_ts)
    ORDER BY (entity_type, entity_id, event_ts, event_id)
    PARTITION BY toYYYYMM(event_ts);

events_hourly AggregatingMergeTree
    ORDER BY (entity_type, entity_id, hour)
    TTL hour + INTERVAL 90 DAY DELETE;

events_hourly_mv → events_hourly AS SELECT
    entity_type, entity_id, toStartOfHour(event_ts) AS hour,
    uniqExactState(event_id)  AS uniq_events,
    sumIf(speed_kmh, IS NOT NULL) AS sum_speed,
    countIf(speed_kmh IS NOT NULL) AS speed_count
FROM raw_events GROUP BY entity_type, entity_id, hour;

-- Canonical read views (hide FINAL / argMax / uniqExactMerge from callers)
raw_events_final    = SELECT * FROM raw_events FINAL
events_hourly_final = uniqExactMerge + sum_speed/speed_count
```

`uniqExactState` instead of `count()` is load-bearing: at-least-once
redelivery writes duplicate rows, and a `SummingMergeTree` MV would
double-count per INSERT block. `uniqExact*` collapses by `event_id` at
query time so counts match Postgres.

### Iceberg — long-term lake (`services/archiver/archiver.py`)

- Table: `mobility.raw_events`, 12 columns, explicit IDs 1–12 for schema
  evolution.
- Partition spec: `days(event_ts)` (hidden) + `identity(entity_type)`.
- At-least-once, dedup-at-read-time by `event_id` (Iceberg has no native
  uniqueness).
- Storage: `s3://lake/` on MinIO; Iceberg REST catalog is the metadata
  authority.

## Quick start

Prereqs: Docker (with `docker compose`), Go 1.25+, `uv` (for the Python tooling).

```bash
# 1. Bring up the local infra stack (Kafka, SR, Postgres, ClickHouse, MinIO, Iceberg REST)
make up

# 2. Register the canonical Avro schema with Schema Registry
make register-schemas

# 3. Run the four Go services (in separate terminals or backgrounded)
go run ./cmd/ingest-api       &
go run ./cmd/stream-processor &
go run ./cmd/query-api        &
go run ./cmd/clickhouse-sink  &

# 4. Emit synthetic events (with duplicate + out-of-order injection for dedupe testing)
make generator-install
cd tools/generator && uv run python gen.py \
  --rate 50 --duration 10 --entities 5 \
  --duplicates 0.20 --out-of-order 0.15 \
  --target http://localhost:8080/events:batch

# 5. Query the hot-state store
curl -s http://localhost:8090/entities/vehicle/vehicle-0/position | jq
curl -s 'http://localhost:8090/entities/vehicle/vehicle-0/events?limit=5' | jq

# 6. When you're done
make down       # keep volumes
make down-v     # wipe volumes (destructive)
```

`make help` lists every target.

## End-to-end verification

After running the generator burst above, the six invariants should all hold:

```sql
-- I1: Postgres dedupe
SELECT COUNT(*) AS total, COUNT(DISTINCT event_id) AS distinct FROM raw_events;

-- I2: out-of-order gate — every entity's position matches the max raw event_ts
SELECT r.entity_type, r.entity_id,
       MAX(r.event_ts) AS max_raw, p.event_ts AS pos,
       (MAX(r.event_ts) = p.event_ts) AS ok
FROM raw_events r JOIN entity_positions p USING (entity_type, entity_id)
GROUP BY r.entity_type, r.entity_id, p.event_ts;

-- I4: ClickHouse distinct count parity with Postgres
SELECT count(), uniqExact(event_id) FROM mobility.raw_events_final;
SELECT entity_type, entity_id, hour, event_count, avg_speed_kmh
FROM mobility.events_hourly_final ORDER BY entity_type, entity_id, hour;
```

Expected after a 10-second burst with `--duplicates 0.2 --out-of-order 0.15`
(310 emitted, 62 dup, 33 OOO): Postgres + ClickHouse each hold 248 distinct
events; hourly rollup sums to 248; no entity has a position older than its
max raw event.

## Service endpoints (local)

| Service | Endpoint | Notes |
|---------|----------|-------|
| ingest-api | <http://localhost:8080> | `POST /events`, `GET /health` |
| query-api | <http://localhost:8090> | `GET /entities/{type}/{id}/position` and `/events` |
| Kafka broker (host) | `localhost:29092` | `PLAINTEXT_HOST` listener for `kcat`/CLI |
| Schema Registry | <http://localhost:8081> | Confluent CP |
| Kafka UI | <http://localhost:8088> | Provectus |
| Postgres | `postgres://postgres:postgres@localhost:5432/mobility` | |
| ClickHouse HTTP | <http://localhost:8123> | default user, no password |
| ClickHouse native | `tcp://localhost:9000` | |
| MinIO API | <http://localhost:9100> | `minioadmin` / `minioadmin` |
| MinIO Console | <http://localhost:9101> | |
| Iceberg REST | <http://localhost:8181> | `s3://lake/` warehouse |
| Prometheus `/metrics` per service | `:9464` / `:9465` / `:9466` / `:9467` | ingest / query / processor / ch-sink |

Optional observability stack (Prometheus + Grafana + Jaeger + OTel Collector):

```bash
docker compose \
  -f deploy/docker-compose.yml \
  -f deploy/observability/docker-compose.observability.yml up -d
# Grafana: http://localhost:3000 (anonymous Viewer, admin/admin for edit)
# Jaeger:  http://localhost:16686
# Prom:    http://localhost:9090
```

## Configuration

All services read env vars via `internal/config`.

| Variable | Used by | Default | Purpose |
|---|---|---|---|
| `HTTP_PORT` | ingest-api | `8080` | HTTP listen |
| `QUERY_HTTP_PORT` | query-api | `8090` | HTTP listen |
| `PROM_PORT` | all Go services | `9464`/`9465`/`9466`/`9467` | Prometheus `/metrics` port (per service) |
| `KAFKA_BROKERS` | all | `localhost:29092` | Comma-separated bootstrap list |
| `KAFKA_TOPIC` | all | `movement.events` | Source topic |
| `KAFKA_GROUP_ID` | stream-processor / clickhouse-sink | `mobility-postgres` / `mobility-clickhouse` | Consumer group |
| `SCHEMA_REGISTRY_URL` | ingest-api | `http://localhost:8081` | SR endpoint |
| `POSTGRES_DSN` | stream-processor / query-api | `postgres://postgres:postgres@localhost:5432/mobility?sslmode=disable` | DSN; **logged with password stripped** via `config.RedactDSN` |
| `CLICKHOUSE_ADDR` / `CLICKHOUSE_DB` | clickhouse-sink | `localhost:9000` / `mobility` | Native protocol |
| `CLICKHOUSE_BATCH_SIZE` / `CLICKHOUSE_BATCH_TIMEOUT` / `CLICKHOUSE_BUFFER_MAX` | clickhouse-sink | `5000` / `2s` / `20000` | Batch + backpressure tuning |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | all | `http://localhost:4318` | OTLP/HTTP receiver |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` / `ICEBERG_CATALOG_URI` / `ICEBERG_CATALOG_TOKEN` | archiver / lake-query | (**no default for secrets**) | Iceberg side |

pgx pool defaults: writer `MaxConns=10`, reader `MaxConns=20`;
`MaxConnLifetime=30m`, `MaxConnIdleTime=5m`, `HealthCheckPeriod=30s`. All
override-able via `pool_*` DSN params.

## Tech choices

| Concern | Choice | Why |
|---------|--------|-----|
| Ingestion API language | Go + chi | Idiomatic, lightweight, matches Dream's primary language |
| Message bus | Apache Kafka (KRaft) | Replay, decoupling, partitioned ordering |
| Schema | Avro + Confluent Schema Registry | Evolution + compact wire format |
| Stream processor | Plain Go consumer (segmentio/kafka-go) | No JVM runtime; state lives in Postgres |
| Hot-state store | Postgres 16 | Atomic dedupe + `event_ts`-gated upsert in one `ON CONFLICT` transaction |
| Analytical store | ClickHouse | Columnar scans; `ReplacingMergeTree` for dedupe; `AggregatingMergeTree` MV stays correct under redelivery |
| Lake storage | MinIO + Apache Iceberg + Parquet | Long retention, schema-evolution, DuckDB/Trino-friendly |
| Python role | Event generator + Iceberg archiver + DuckDB CLI | PyIceberg is more ergonomic than Go for Iceberg writes |
| Orchestration | docker-compose primary; Helm charts for kind | Fast iteration locally; Helm covers the K8s path |
| Observability | OpenTelemetry → Prometheus + Grafana + Jaeger | One SDK, three signals; end-to-end traces on demand |
| Containers | distroless + non-root UID 65532 | Smallest attack surface |

## Operational properties

- **Delivery**: **at-least-once everywhere**. Dedupe is the sink's
  responsibility (`event_id` PK / `ReplacingMergeTree` / read-time GROUP BY).
- **Shutdown**: every service uses `signal.NotifyContext` + a `run() error`
  helper so defers fire. Kafka commits use a detached 5s context so
  SIGTERM doesn't orphan the last message. HTTP servers drain via
  `Shutdown(10s)`.
- **Error handling**: poison messages are committed past and counted
  (`decodeFailures` counter surfaced by the 30s stats sampler). Broker
  fetch errors get a 500 ms backoff to avoid CPU pinning.
- **Observability**: every Go service exports Prometheus `/metrics` on its
  own port + OTLP traces when the opt-in observability compose is running.
  Structured JSON logs with redacted DSNs.
- **Security**: all binaries run as non-root (UID 65532) in distroless
  containers. Helm charts set pod-level `runAsNonRoot: true`,
  `seccompProfile: RuntimeDefault`, container-level
  `readOnlyRootFilesystem: true`, `allowPrivilegeEscalation: false`,
  `capabilities.drop: [ALL]`. `postgresSecret` value pattern for
  production DSN via `secretKeyRef`. `govulncheck` clean.

### Known limits (deferred)

- No auth on ingest-api / query-api — expected to sit behind an API gateway.
- Iceberg archiver is at-least-once with dedup-at-read (no unique-by-event_id
  on the lake side yet).
- Single Kafka broker, RF=1 — dev-only. Production needs RF≥3 + mTLS +
  SASL + an idempotent producer.
- Observability stack is an opt-in compose override; Helm charts do not
  install Prometheus/Grafana today.

## Phases

Built phase-by-phase. Each phase closed with an
[audit round](.claude/commands/audit.md) run by four specialized
[reviewer agents](.claude/agents/) (Go, streaming, storage, infra). Every
🚨 Critical was fixed before the next phase began.

| # | Scope | Audit-surfaced highlight |
|---|---|---|
| 0 | Repo scaffold + compose + Python generator | — |
| 1 | Avro schema + Go codegen | — |
| 2 | Ingestion API + Kafka producer (SR wire format) | Producer registered a stripped schema variant; broken `v2` in SR blocking evolution — fixed via embedded canonical `.avsc`. |
| 3 | Stream processor → Postgres | Detached commit context, fetch backoff, decode-failures counter, `event_ts` column rename, lat/lon CHECK constraints. |
| 4 | Query API (composite-cursor pagination) | Keyset pagination tie-break (composite cursor), pool config, per-query timeouts, JSONB as `json.RawMessage`. |
| 5 | ClickHouse sink + hourly rollup MV | Batch-aligned offset commit (offsets moved into the consumer loop; was a silent data-loss path), `AggregatingMergeTree` + `uniqExactState` for dedupe-safe counts. |
| 6 | PyIceberg archiver + DuckDB CLI | Narrowed `except Exception` swallow, catalog-token support, removed hardcoded creds, secret guardrails in lake-query. |
| 7 | Dockerfiles + Helm charts + kind scripts | Missing Dockerfiles + security context + probes on workers + secret-ref DSN pattern. |
| 8 | OpenTelemetry SDK + collector/Prom/Grafana | Wired `otel.Init` into every `cmd/main.go` (was a dead listener); Prometheus server timeouts; partial-init leak fix. |
| 9 | k6 load + chaos scenarios | Chaos scenario 4 was testing nothing (processors were in different groups) — fixed. |

## Repo layout

```
.
├── cmd/                    # Go binaries (+ Dockerfile per service)
│   ├── ingest-api/
│   ├── stream-processor/
│   ├── query-api/
│   └── clickhouse-sink/
├── internal/               # Go packages
│   ├── api/                # ingest-api HTTP handler + validation
│   ├── avro/               # generated Avro struct + embedded .avsc
│   ├── chsink/             # ClickHouse batched sink
│   ├── config/             # env loader + RedactDSN
│   ├── otel/               # shared OTel SDK init
│   ├── processor/          # Kafka consumer + Postgres store
│   ├── producer/           # Kafka producer with SR wire format
│   └── query/              # query-api handler, store, cursor
├── services/
│   └── archiver/           # Python Kafka→Iceberg archiver
├── tools/
│   ├── generator/          # Python synthetic event generator
│   └── lake-query/         # Python DuckDB-over-Iceberg CLI
├── deploy/
│   ├── docker-compose.yml          # Local infra stack
│   ├── observability/              # Opt-in OTel + Prom + Grafana + Jaeger
│   └── helm/                       # Helm charts (one per Go service)
├── loadtest/               # k6 scripts + chaos scenarios
├── scripts/                # Shell helpers (register-schemas, kind-up, smoke, ...)
├── .claude/                # Reviewer agents + /audit slash command
├── Makefile
└── README.md
```

## Testing

```bash
go test -race -count=1 ./...                                   # unit tests
go test -race -count=1 -tags=integration ./internal/...        # testcontainers-go
make smoke                                                      # ingest-api smoke
make sr-state                                                   # Schema Registry state dump
make sr-state -- --probe-evolution                              # BACKWARD-compat probe
```

## Troubleshooting

- **`make up` hangs on a service**: `docker compose -f deploy/docker-compose.yml -p dream-mobility logs <service>`. Healthchecks have generous timeouts but a wedged container will block `--wait`.
- **Port already in use**: most likely Postgres (5432), ClickHouse (9000 native), MinIO (9100), or a Prometheus port (9464–9467) from a stray `go run`. `lsof -ti:<port> | xargs kill` clears it.
- **ClickHouse sink won't start — "Database mobility does not exist"**: the compose volume is from an older schema. Run `make down-v && make up` to rebuild.
- **Wipe everything and start fresh**: `make down-v && make up`.
