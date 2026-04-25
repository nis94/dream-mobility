# Dream Flight — Real-Time Aircraft Telemetry

A backend pipeline that ingests live aircraft state vectors from
[OpenSky Network](https://openskynetwork.github.io/opensky-api/), validates
and normalizes them into a typed `FlightTelemetry` schema, and fans them out
to three sink stores so downstream applications can answer different
questions: latest known state per aircraft, country/airline analytics, and
long-term lake replay.

Built as a personal learning project. The same data goes to three places
because each excels at a different query shape — point lookups for hot
state, columnar aggregations for dashboards, schema-evolved Parquet for
the lake.

Stack: Go, Python, Kafka + Confluent Schema Registry + Avro, Postgres,
ClickHouse, MinIO + Apache Iceberg, Kubernetes + Helm + ArgoCD,
OpenTelemetry.

## Capabilities

| Capability | Path | Latency target |
|---|---|---|
| Real-time ingest from OpenSky | `opensky-ingest` polls `/states/all` every 300s | sub-second per poll |
| Push-side ingest (smoke / chaos / external apps) | HTTP `POST /events` on ingest-api | <50 ms p99 |
| Deliver to every downstream store | Kafka `flight.telemetry` → independent consumer groups | seconds |
| Last-known state for an aircraft | HTTP `GET /aircraft/{icao24}` | <20 ms p99 (point read) |
| Currently active aircraft (last 5 min, optional country filter) | HTTP `GET /flights/active?origin_country=GB` | <50 ms p99 |
| Per-aircraft trajectory | HTTP `GET /aircraft/{icao24}/track?limit=&cursor=` | <100 ms p99 |
| Country / airline / airspace-grid analytics | ClickHouse `*_final` rollup views | sub-second scans |
| Long-term lake replay | Iceberg `flight.telemetry` + DuckDB / Trino / Spark | minutes |

The pipeline is deliberately **three-store** — Postgres for hot state,
ClickHouse for dashboards, Iceberg for the lake — because each answers a
different question better than the other two. Kafka is the fan-out point.

## Architecture

```mermaid
flowchart LR
    EXT[External Clients] -->|HTTP POST /events| API[ingest-api<br/>Go + chi]
    GEN[Python Synthetic<br/>Generator] -->|HTTP| API
    API -->|Avro + SR wire format| KAFKA[(Kafka topic<br/>flight.telemetry<br/>key = icao24)]
    SR[(Schema Registry)] -.->|schema id cache| API

    KAFKA --> PROC[stream-processor<br/>group: flight-postgres]
    KAFKA --> CHSINK[clickhouse-sink<br/>group: flight-clickhouse<br/>batched, per-batch commit]
    KAFKA --> ARCH[archiver<br/>group: flight-iceberg<br/>PyIceberg]

    PROC -->|ON CONFLICT + tx| PG[(Postgres<br/>flight_telemetry +<br/>aircraft_state)]
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
    participant K as Kafka<br/>flight.telemetry
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
    A->>K: WriteMessages key=icao24 (Murmur2)
    A-->>C: 202 {accepted, rejected, errors[]}

    par independent consumer groups
        K->>P: FetchMessage(flight-postgres)
        P->>P: decode SR wire format
        P->>PG: BEGIN; INSERT flight_telemetry ON CONFLICT; UPSERT aircraft_state (timestamp-gated)
        PG-->>P: committed
        P->>K: CommitMessages (detached 5s ctx)
    and
        K->>H: FetchMessage(flight-clickhouse)
        H->>H: buffer up to batch_size or batch_timeout
        H->>CH: prepareBatch / Append / Send
        CH-->>H: ok
        H->>K: CommitMessages for the whole batch
    and
        K->>I: poll(flight-iceberg)
        I->>I: buffer + Arrow RecordBatch
        I->>L: table.append(parquet)
        I->>K: commit offsets
    end

    Note over C,Q: read path
    C->>Q: GET /aircraft/abc123
    Q->>PG: point read aircraft_state
    Q-->>C: 200 {icao24, last_callsign, lat, lon, observed_at, last_event_id}
    C->>Q: GET /aircraft/abc123/track?limit=50
    Q->>PG: row-wise keyset before cursor (observed_at, event_id)
    Q-->>C: 200 {telemetry[], next_cursor}
```

The key invariant running through every arrow: **the Kafka offset advances
only after the downstream store has durably accepted the write.** A crash
between DB commit and offset commit replays the last message — and the
sink-side idempotence keys (`event_id` in Postgres / ClickHouse / read-time
GROUP BY in Iceberg) make that replay a no-op.

## Correctness invariants

| # | Invariant | Enforced by |
|---|---|---|
| I1 | Every observation has a unique `event_id` post-dedupe in Postgres | `flight_telemetry.event_id PRIMARY KEY` + `ON CONFLICT DO NOTHING` |
| I2 | `aircraft_state.observed_at` equals `MAX(flight_telemetry.observed_at)` per `icao24` | `WHERE EXCLUDED.observed_at > aircraft_state.observed_at` in the upsert |
| I3 | Kafka offset for a message advances only after its downstream write commits | `FetchMessage` → write → `CommitMessages`, with detached context for the commit so SIGTERM doesn't leave the last offset behind |
| I4 | ClickHouse `active_aircraft` (from `*_hourly_*_final` rollups) equals distinct `icao24` count in the matching raw time range | `AggregatingMergeTree` with `uniqExactState(icao24)` + read-side `uniqExactMerge` |
| I5 | Paginated track: every `event_id` for a given `icao24` is served at most once, including observations sharing the same microsecond | Composite `(observed_at, event_id)` cursor + row-wise `< (ts, id)` SQL + `ORDER BY observed_at DESC, event_id DESC` |
| I6 | Per-aircraft ordering is preserved into each sink | Producer Murmur2 key = `icao24` → same partition → single consumer owns the partition → sequential reads |

Integration tests (`internal/processor/store_test.go`, build tag
`integration`) verify I1/I2 against a `testcontainers-go` Postgres. Unit
tests verify I5 (`TestListEvents_TieBreakAcrossPages`). The end-to-end
smoke below verifies all six together on a live stack.

## Data model

### Avro wire contract (Kafka + Schema Registry)

`internal/avro/flight_telemetry.avsc` — 17 fields, every one typed (no
attributes grab-bag). Maps directly to OpenSky's `/states/all` row shape so
the producer is a thin adapter.

```text
record FlightTelemetry {
    event_id:         string  (logicalType=uuid)        // UUIDv4 per emission; dedupe key
    icao24:           string                             // 6-char lowercase hex; partition key
    callsign:         [null, string]                    // BAW123 / ferry / null
    origin_country:   string                             // operator country (Iceberg partition)
    observed_at:      long    (logicalType=timestamp-micros)
    position_source:  enum {ADSB, ASTERIX, MLAT, FLARM, UNKNOWN}
    lat, lon:         double
    baro_altitude_m:  [null, double]                    // both altimeters kept — they disagree
    geo_altitude_m:   [null, double]
    velocity_ms:      [null, double]                    // ground speed; native OpenSky unit
    true_track_deg:   [null, double]                    // 0..359, 0=N
    vertical_rate_ms: [null, double]                    // climb (+) / descent (-)
    on_ground:        boolean
    squawk:           [null, string]                    // 4-octal-digit transponder
    spi:              boolean                            // special-purpose indicator
    category:         [null, int]                       // OpenSky aircraft category 0..20
}
```

Topic: `flight.telemetry` (3 partitions, `key=icao24`). Wire format:
`0x00` magic + big-endian uint32 schema ID + Avro binary payload.

### Postgres — hot state (`internal/processor/schema.sql`)

```sql
flight_telemetry (
    event_id        UUID PRIMARY KEY,
    icao24, callsign, origin_country, position_source,
    observed_at     TIMESTAMPTZ,
    lat, lon, baro_altitude_m, geo_altitude_m,
    velocity_ms, true_track_deg, vertical_rate_ms,
    on_ground, squawk, spi, category, ingested_at,
    CHECK (lat BETWEEN -90 AND 90), CHECK (lon BETWEEN -180 AND 180)
);
INDEX (icao24, observed_at DESC);
INDEX (origin_country, observed_at DESC);   -- powers /flights/active?origin_country=

aircraft_state (
    icao24 PRIMARY KEY,
    last_callsign, origin_country, observed_at,
    lat, lon, baro_altitude_m, velocity_ms, on_ground,
    last_squawk, last_event_id, updated_at
);
-- Upsert: WHERE EXCLUDED.observed_at > aircraft_state.observed_at  (strict >)
```

Strict `>` on the state upsert discards stale observations; `READ COMMITTED`
+ the `WHERE` guard are sufficient under a single consumer.

### ClickHouse — analytics (`internal/chsink/schema.sql`)

Three rollups feed off the raw table via materialized views, each answering
a different operational question.

```sql
flight.telemetry ReplacingMergeTree(observed_at)
    ORDER BY (icao24, observed_at, event_id)
    PARTITION BY toYYYYMM(observed_at)
    TTL toDateTime(observed_at) + INTERVAL 180 DAY;

-- Rollup 1: country-level operations
flight.telemetry_hourly_by_country AggregatingMergeTree
    ORDER BY (origin_country, hour)
    columns: uniq_aircraft, sum/count_velocity, sum/count_baro_alt,
             climbing_count, descending_count, cruising_count,
             on_ground_count, emergency_squawks_count

-- Rollup 2: airline-level tempo (callsign first 3 chars = ICAO airline code)
flight.telemetry_hourly_by_airline AggregatingMergeTree
    ORDER BY (callsign_prefix, hour)
    columns: uniq_aircraft, uniq_callsigns, sum/count_velocity,
             climbing/descending/cruising/on_ground counts

-- Rollup 3: 0.25° airspace-density grid in 5-minute buckets (airborne only)
flight.airspace_grid_5min AggregatingMergeTree
    ORDER BY (bucket_5min, lat_cell, lon_cell)
    columns: uniq_aircraft

-- Each gets a *_final view that uniqExactMerge / sums for read-time scalars
flight.telemetry_final, *_hourly_by_country_final, *_hourly_by_airline_final, *_grid_5min_final
```

`uniqExactState(icao24)` instead of `count()` is load-bearing: at-least-once
redelivery writes duplicate rows, and a `SummingMergeTree` MV would
double-count per INSERT block. `uniqExact*` collapses at query time so
active-aircraft counts match Postgres.

### Iceberg — long-term lake (`services/archiver/archiver.py`)

- Table: `flight.telemetry`, 18 columns, explicit field IDs 1–18 for schema evolution.
- Partition spec: `days(observed_at)` (hidden) + `identity(origin_country)`.
  "All UK flights last week" prunes ~95% of files.
- Sort order within partition: `observed_at ASC` for tight Parquet row-group
  min/max pruning under out-of-order ingest.
- At-least-once, with in-batch dedup by `event_id` at flush; cross-batch
  dedup is read-time (`SELECT DISTINCT ON (event_id)` or equivalent).
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

# 4a. Pull live data from OpenSky (the primary path)
cd services/opensky-ingest && uv sync
KAFKA_BROKERS=localhost:29092 \
SCHEMA_REGISTRY_URL=http://localhost:8081 \
uv run python opensky_ingest.py    # polls every 300s; ctrl-c to stop

# 4b. Or generate synthetic chaos data (dupes + out-of-order — only deterministic way to test those)
make generator-install
cd tools/generator && uv run python gen.py \
  --rate 50 --duration 10 --entities 5 \
  --duplicates 0.20 --out-of-order 0.15 \
  --target http://localhost:8080/events:batch

# 5. Query the hot-state store
curl -s http://localhost:8090/aircraft/abc123 | jq
curl -s 'http://localhost:8090/flights/active?origin_country=United%20Kingdom' | jq
curl -s 'http://localhost:8090/aircraft/abc123/track?limit=10' | jq

# 6. When you're done
make down       # keep volumes
make down-v     # wipe volumes (destructive)
```

`make help` lists every target.

## End-to-end verification

After ingesting some data (live from OpenSky, or a generator burst), the
six invariants should all hold:

```sql
-- I1: Postgres dedupe
SELECT count(*) AS total, count(DISTINCT event_id) AS distinct FROM flight_telemetry;

-- I2: out-of-order gate — every aircraft's state matches its MAX raw observed_at
SELECT r.icao24,
       MAX(r.observed_at) AS max_raw, s.observed_at AS state,
       (MAX(r.observed_at) = s.observed_at) AS ok
FROM flight_telemetry r JOIN aircraft_state s USING (icao24)
GROUP BY r.icao24, s.observed_at;

-- I4: ClickHouse distinct-count parity with Postgres
SELECT count(), uniqExact(event_id) FROM flight.telemetry_final;
SELECT origin_country, hour, active_aircraft, avg_velocity_ms
FROM flight.telemetry_hourly_by_country_final
ORDER BY hour DESC, active_aircraft DESC LIMIT 10;
```

Expected after a 5-minute OpenSky poll: ~5,000-10,000 distinct `icao24` rows
in `aircraft_state`, the same row count in `flight.telemetry` ± broker-retry
duplicates (the `_final` view collapses them). `_hourly_by_country` shows
the leaderboard of operator countries — typically US, UK, Germany on top.

## Service endpoints (local)

| Service | Endpoint | Notes |
|---------|----------|-------|
| ingest-api | <http://localhost:8080> | `POST /events`, `GET /health` |
| query-api | <http://localhost:8090> | `GET /aircraft/{icao24}`, `/aircraft/{icao24}/track`, `/flights/active` |
| Kafka broker (host) | `localhost:29092` | `PLAINTEXT_HOST` listener for `kcat`/CLI |
| Schema Registry | <http://localhost:8081> | Confluent CP — subject `flight.telemetry-value` |
| Kafka UI | <http://localhost:8088> | Provectus |
| Postgres | `postgres://postgres:postgres@localhost:5432/flight` | |
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
| `KAFKA_TOPIC` | all | `flight.telemetry` | Source topic |
| `KAFKA_GROUP_ID` | stream-processor / clickhouse-sink / archiver | `flight-postgres` / `flight-clickhouse` / `flight-iceberg` | Consumer group |
| `SCHEMA_REGISTRY_URL` | ingest-api / opensky-ingest | `http://localhost:8081` | SR endpoint |
| `POSTGRES_DSN` | stream-processor / query-api | `postgres://postgres:postgres@localhost:5432/flight?sslmode=disable` | DSN; **logged with password stripped** via `config.RedactDSN` |
| `CLICKHOUSE_ADDR` / `CLICKHOUSE_DB` | clickhouse-sink | `localhost:9000` / `flight` | Native protocol |
| `POLL_INTERVAL_SECONDS` | opensky-ingest | `300` | OpenSky polling cadence; anonymous limit 400 credits/day → don't drop below 215s |
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
| Hot-state store | Postgres 16 | Atomic dedupe + `observed_at`-gated upsert in one `ON CONFLICT` transaction |
| Analytical store | ClickHouse | Columnar scans; `ReplacingMergeTree` for dedupe; `AggregatingMergeTree` MV stays correct under redelivery |
| Lake storage | MinIO + Apache Iceberg + Parquet | Long retention, schema-evolution, DuckDB/Trino-friendly |
| Live data source | `opensky-ingest` (Python) polling OpenSky `/states/all` direct → Kafka | Pull-based source has no reason to round-trip through HTTP; ingest-api stays for push clients |
| Python role | Live producer + Iceberg archiver + chaos generator + DuckDB CLI | PyIceberg + confluent-kafka-python are more ergonomic for Iceberg + adapter shapes |
| Orchestration | docker-compose primary; Helm charts + ArgoCD for kind | Fast iteration locally; ArgoCD app-of-apps is the declarative path |
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
| 3 | Stream processor → Postgres | Detached commit context, fetch backoff, decode-failures counter, `observed_at` column rename, lat/lon CHECK constraints. |
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
│   ├── archiver/           # Python Kafka→Iceberg archiver (group: flight-iceberg)
│   └── opensky-ingest/     # Python OpenSky→Kafka producer (live data path)
├── tools/
│   ├── generator/          # Synthetic chaos generator (--duplicates / --out-of-order)
│   └── lake-query/         # DuckDB-over-Iceberg CLI
├── deploy/
│   ├── docker-compose.yml          # Local infra stack (kafka, postgres, ch, minio, iceberg-rest)
│   ├── observability/              # Opt-in OTel + Prom + Grafana + Jaeger
│   ├── helm/                       # Helm charts (Go services + 2 Python services)
│   └── argocd/                     # App-of-apps: 6 ArgoCD Applications
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

- **`make up` hangs on a service**: `docker compose -f deploy/docker-compose.yml -p dream-flight logs <service>`. Healthchecks have generous timeouts but a wedged container will block `--wait`.
- **Port already in use**: most likely Postgres (5432), ClickHouse (9000 native), MinIO (9100), or a Prometheus port (9464–9467) from a stray `go run`. `lsof -ti:<port> | xargs kill` clears it.
- **ClickHouse sink won't start — "Database flight does not exist"**: the compose volume is from an older schema. Run `make down-v && make up` to rebuild.
- **Wipe everything and start fresh**: `make down-v && make up`.
