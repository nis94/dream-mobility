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

## Architecture

```mermaid
flowchart LR
    EXT[External Clients] -->|HTTP POST /events| API[Ingestion API<br/>Go + chi]
    GEN[Python Synthetic<br/>Generator] -->|HTTP| API
    API -->|Avro + SR| KAFKA[(Kafka topic:<br/>movement.events<br/>partitioned by entity.id)]
    SR[(Schema Registry)] -.->|fetch schemas| API
    SR -.-> PROC
    SR -.-> ARCH

    KAFKA --> PROC[Stream Processor<br/>Go consumer<br/>dedupe + upsert]
    KAFKA --> CHSINK[ClickHouse Sink<br/>Go consumer<br/>batched inserts]
    KAFKA --> ARCH[PyIceberg Archiver<br/>Python consumer]

    PROC -->|ON CONFLICT| PG[(Postgres<br/>raw_events +<br/>entity_positions)]
    CHSINK --> CH[(ClickHouse<br/>raw + materialized<br/>views for rollups)]
    ARCH -->|Parquet| MINIO[(MinIO / S3)]
    ARCH -.->|register| ICAT[Iceberg REST Catalog]

    PG --> QAPI[Query API<br/>Go]
    CH --> QAPI

    QAPI --> CLI[curl / clients]

    PROC -.->|metrics+traces| OTEL[OpenTelemetry Collector]
    API -.-> OTEL
    CHSINK -.-> OTEL
    OTEL --> PROM[Prometheus + Grafana]
```

## Quick start

Prereqs: Docker (with `docker compose`), Go 1.22+, `uv` (for the Python generator).

```bash
# 1. Bring up the local infra stack (Kafka, SR, Postgres, ClickHouse, MinIO, Iceberg REST)
make up

# 2. Install the synthetic event generator
make generator-install

# 3. Emit some events to stdout (Phase 0 verification)
make generator-run ARGS="--rate 50 --duration 5 --duplicates 0.05 --out-of-order 0.10"

# 4. When you're done
make down       # keep volumes
make down-v     # wipe volumes (destructive)
```

`make help` lists every target.

## Service endpoints (local)

| Service | Endpoint | Notes |
|---------|----------|-------|
| Kafka broker (host) | `localhost:29092` | `PLAINTEXT_HOST` listener for `kcat`/CLI |
| Schema Registry | <http://localhost:8081> | Confluent CP |
| Postgres | `postgres://postgres:postgres@localhost:5432/mobility` | |
| ClickHouse HTTP | <http://localhost:8123> | default user, no password |
| ClickHouse native | `tcp://localhost:9000` | |
| MinIO API | <http://localhost:9100> | `minioadmin` / `minioadmin` |
| MinIO Console | <http://localhost:9101> | |
| Iceberg REST | <http://localhost:8181> | `s3://lake/` warehouse |
| Kafka UI | <http://localhost:8088> | Provectus |

## Tech choices

| Concern | Choice | Why |
|---------|--------|-----|
| Ingestion API language | Go + chi | Idiomatic, lightweight, matches Dream's primary language |
| Message bus | Apache Kafka (KRaft mode) | Replay, decoupling, partitioned ordering |
| Schema | Avro + Confluent Schema Registry | Schema evolution, compact wire format, native SR integration |
| Stream processor | Plain Go consumer (`confluent-kafka-go` v2) | Kafka Streams is JVM-only; Flink/Spark are too heavy for this scope; state lives in Postgres |
| Hot-state store | Postgres 16 | Atomic dedupe + timestamp-gated upsert in one `ON CONFLICT` statement |
| Analytical store | ClickHouse | Columnar, fast time-series scans; `ReplacingMergeTree` handles dedupe at compaction |
| Lake storage | MinIO + Apache Iceberg + Parquet | Long retention, schema-evolution, ad-hoc analytics surface |
| Python role | Async generator + PyIceberg archiver | Generator gives realistic test traffic; PyIceberg is more ergonomic than Go for Iceberg writes |
| Orchestration | docker-compose primary; Helm charts later | Fast iteration locally; Helm covers K8s exposure |
| Observability | OpenTelemetry → Prometheus + Grafana | One SDK, three signals; end-to-end traces for demos |

A deeper write-up of tradeoffs lives in `DESIGN.md` (added as the project grows).

## Roadmap (phased)

The project is built phase-by-phase. Each phase ends with a runnable artifact and a verification step. The full plan is at `~/.claude/plans/swift-waddling-fountain.md`.

| Phase | Scope | Status |
|-------|-------|--------|
| 0 | Repo scaffold + docker-compose + synthetic generator | in progress |
| 1 | Avro schemas + Go codegen | |
| 2 | Ingestion API + Kafka producer | |
| 3 | Stream processor → Postgres (dedupe + upsert) | |
| 4 | Query API (MVP completion) | |
| 5 | ClickHouse sink + materialized rollups | |
| 6 | PyIceberg archiver + MinIO/Iceberg lake | |
| 7 | Helm charts + kind cluster | |
| 8 | Observability (OpenTelemetry → Prom/Grafana) | |
| 9 | Load + chaos testing (k6) | |

## Repo layout

```
.
├── cmd/                    # Go binaries (one per service, added phase-by-phase)
├── internal/               # Go packages
├── schemas/                # Avro schemas
├── migrations/             # Postgres + ClickHouse SQL
├── services/               # Non-Go services (Python archiver in phase 6)
├── tools/                  # Dev tooling
│   └── generator/          # Phase 0 -- synthetic event generator
├── deploy/                 # Infra
│   └── docker-compose.yml  # Local stack
├── scripts/                # Helper shell scripts
├── e2e/                    # End-to-end tests (added phase 3)
├── loadtest/               # k6 scripts (added phase 9)
├── Makefile
└── README.md
```

## Troubleshooting

- **`make up` hangs on a service**: `docker compose -f deploy/docker-compose.yml -p dream-mobility logs <service>`. Healthchecks have generous timeouts but a wedged container will block `--wait`.
- **Port already in use**: most likely Postgres (5432), ClickHouse (9000 native), or MinIO (9100). Stop the conflicting local process or remap the port in `deploy/docker-compose.yml`.
- **Wipe everything and start fresh**: `make down-v && make up`.
