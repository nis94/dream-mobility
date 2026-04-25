# Dream Flight — a tour

A story-shaped walkthrough of the whole system: what we built, why we
picked each tool, and what role each piece plays. Read top to bottom in
one sitting; use it to decide which corner of the codebase is worth
reading in detail.

[README.md](./README.md) is the operational reference — make targets,
invariants, endpoints. This file is the narrative underneath it.

---

## 1. The problem

Imagine a fleet of vehicles, scooters, couriers, and delivery devices
all pinging their location several times a second. Three different
people want three different things out of that stream:

- **An operations app** wants to open a dashboard and see "where is
  vehicle V7 right now?" — single lookup, must return in
  milliseconds.
- **A product manager** wants hourly heatmaps, average speeds per
  region, week-over-week comparisons — range scans over large time
  windows, acceptable in sub-second to a few seconds.
- **A data scientist** wants to replay the last year's movements to
  train a model — full scans over massive historical data, takes
  minutes, runs ad hoc.

No single database is good at all three. Postgres is perfect for the
first and awful for the third. ClickHouse is perfect for the second
and mediocre for the first. A data lake is perfect for the third and
terrible for the first two.

So the whole project is a lesson in **writing once, reading three ways**
— build a pipeline that ingests events exactly once from the outside
world, then fans them out to three purpose-built stores, each optimized
for one of those queries.

---

## 2. The shape of the answer

We land every inbound event into **one message bus** (Kafka), and from
there three independent consumers each write into their own store.
Every consumer is idempotent on a stable `event_id`, so the whole
pipeline is at-least-once end to end — if anything crashes and a
message is redelivered, the write is a no-op.

```
HTTP → ingest-api → Kafka (flight.telemetry, 3 partitions)
                      │
          ┌───────────┼───────────┐
          ▼           ▼           ▼
     Postgres    ClickHouse    Iceberg (MinIO)
     (hot state) (analytics)   (lake)

                 ▲                 ▲
                 │                 │
              query-api      lake-query CLI
              (HTTP)         (DuckDB over Iceberg)
```

Each arrow in that diagram is a choice we made, and the rest of this
document explains each choice.

---

## 3. The services we wrote

The pipeline has six long-running services — four Go and two Python —
plus two Python CLI helpers. Each one owns exactly one thing.

### Go services (four, under `cmd/`)

**`ingest-api`** — the HTTP front door. Accepts `POST /events`, validates
the payload, serializes to Avro with the Confluent Schema Registry
wire format, and writes to Kafka. Returns `202 Accepted`. Nothing
more — no DB writes, no business logic, just "get bytes off the wire
and onto the bus as fast as possible."

**`stream-processor`** — the Postgres writer. Reads Kafka, inserts into
`flight_telemetry` (dedupe by `event_id`), upserts `aircraft_state` with a
timestamp guard so out-of-order events don't regress the last-known
position. Kafka offset advances only after the Postgres transaction
commits; a crash replays the last message and idempotence absorbs it.

**`clickhouse-sink`** — the ClickHouse writer. Same Kafka topic,
different consumer group, so it gets its own offsets. Buffers
incoming events and flushes in batches (tuneable by env), because
ClickHouse hates single-row inserts. Offsets commit per batch.

**`query-api`** — the read-side HTTP service. Reads Postgres only.
Exposes `GET /aircraft/{icao24}` (last-known state), `/aircraft/{icao24}/track`
(paginated reverse-chronological observations), and `/flights/active`
(active aircraft in the last 5 minutes, optionally filtered by
`origin_country`). The pagination cursor is composite
`(observed_at, event_id)` so ties within the same microsecond don't
duplicate rows across pages.

All four are plain stdlib-ish Go — the only real frameworks are
`chi` for routing (small, idiomatic, standard) and `pgx` for
Postgres (high performance, used by everyone). No ORM, no DI
container, no magic.

### Python services (two)

**`services/opensky-ingest`** — the live data producer. Polls
[OpenSky Network](https://openskynetwork.github.io/opensky-api/)'s
`/api/states/all` every 300 seconds (anonymous tier safe), normalizes
each state vector into the FlightTelemetry record, encodes it as Avro
with the SR wire-format prefix, and produces direct to Kafka keyed by
`icao24`. Bypasses ingest-api on purpose — a pull-based source has no
reason to round-trip through HTTP.

**`services/archiver`** — the Iceberg writer, written in Python because
**PyIceberg** is dramatically more mature than any Go Iceberg client.
Reads from Kafka (same topic, its own consumer group), buffers events
in memory, and flushes them as Parquet files into a partitioned
Iceberg table on MinIO. Uses the Iceberg REST catalog for metadata.

### Python CLI tools (two)

**`tools/generator`** — synthetic event producer used for chaos
testing. Emits random aviation events with injectable duplicate and
out-of-order ratios, so we can verify dedupe and the
timestamp-gated upsert guard actually work. Run on demand against the
ingest-api; not a long-running service.

**`tools/lake-query`** — a DuckDB-over-Iceberg CLI for running ad-hoc
SQL against the lake. Takes one query, runs it locally, returns rows.

### Why this split of languages

Go is great at small, high-throughput network services with
aggressive goroutine use and predictable latency — everything on the
hot path is Go. Python is chosen only where the ecosystem pays
more than the performance loses: PyIceberg makes the archiver
trivial, and for event simulation / ad-hoc querying, iteration speed
beats throughput.

---

## 4. The data plane — Kafka, Schema Registry, and Avro

Between `ingest-api` and the three sinks sits **Kafka**. Kafka is the
single point that guarantees:

- **Durable buffering**. If ClickHouse is down, events pile up in
  Kafka; when it comes back, the `clickhouse-sink` catches up from
  its offset. Postgres's consumer is unaffected.
- **Replay**. At any time we can spin up a new consumer group
  (say, a "recompute the last 30 days into a new store") that reads
  from the earliest offset. No need to re-ingest from the source.
- **Partitioned ordering**. Kafka guarantees total order *within a
  partition*. We key every message by `icao24` using
  the Murmur2 partitioner, so all events for vehicle-7 hit the same
  partition, which means they're delivered in the order they were
  produced, which means out-of-order events (real-world issue on
  flaky mobile networks) only happen from the source side — never
  introduced by the pipeline.

We picked Kafka over (say) NATS or Redis Streams because:
- It has proper multi-consumer semantics (distinct consumer groups,
  each with their own committed offset) — exactly the fan-out
  pattern we need.
- KRaft mode removed the ZooKeeper operational tax. Even a local
  single-broker install is trivial now.
- Every serious streaming book, tool, and employer assumes Kafka.
  Learning it well is a career-ROI choice.

We run **one broker** locally with **replication factor 1** — fine for
dev, would be replaced with three brokers and RF≥3 in production.

### Why Avro and Schema Registry

Events on the wire are not JSON. They're **Avro**, prefixed with the
Confluent Schema Registry wire format (`0x00` magic byte + 4-byte
big-endian schema ID + Avro binary).

Three reasons:

1. **Compact**. An Avro-encoded movement event is roughly 1/3 the
   size of the equivalent JSON. Saves Kafka disk, network, and
   downstream parse cost.
2. **Typed**. Every producer and every consumer agrees on a single
   schema stored in the registry. A consumer that doesn't match the
   schema fails loud, not silent.
3. **Evolvable**. Schema Registry enforces compatibility rules
   (BACKWARD by default for movement events). You can add a
   nullable field, rename-via-alias, drop a default — but you
   cannot make a change that breaks an existing consumer.

Protobuf would give us similar guarantees. We picked Avro mostly
because Confluent's ecosystem (Kafka Connect, ksqlDB, Flink SQL)
speaks Avro natively, and because the wire-format story is simpler
for a Go toolchain than protobuf's schema-sharing patterns.

**Confluent Schema Registry** is where schemas live. It's a single
compose container locally, a managed or self-hosted cluster in
production. Producers register the schema once at startup (and cache
the returned ID); consumers resolve ID → schema lazily.

---

## 5. The three stores — why one isn't enough

### Postgres — hot state

Role: point reads, small range scans, "what's the current state of
entity X?"

We use Postgres 16 with two tables: `flight_telemetry` (dedupe PK on
`event_id`) and `aircraft_state` (one row per entity, timestamp-gated
upsert). The stream-processor writes both in a single transaction, so
the offset only advances after we've durably committed both writes.

Why Postgres and not, say, Redis or Cassandra?
- **Atomicity on multi-row writes**. We need the raw-event dedupe
  INSERT and the entity-position upsert to either both happen or
  neither — a plain SQL transaction gives us that for free.
- **Familiar**. Every backend engineer speaks Postgres. It is the
  least-surprising choice.
- **Indexing for our access patterns is trivial**. The hot read is
  a keyset pagination on `(icao24, observed_at DESC, event_id DESC)` —
  one composite index, done.

What Postgres is bad at, and why we didn't pick it for everything:
scanning a million rows to compute an hourly aggregate would either
take minutes or require cascading materialized views. Point reads
are its sweet spot.

### ClickHouse — dashboards and analytics

Role: range scans, aggregates, "active aircraft per country per hour",
"top airlines by climb/descent rate today", "airspace density over
Europe in the last 5 minutes."

ClickHouse is a columnar OLAP database. It stores data in sorted,
compressed column files, so scanning 10 million rows to sum one
column is ~10× cheaper than in a row store like Postgres.

The schema (in `internal/chsink/schema.sql`) has one raw table plus
three materialized rollups, each answering a different operational
question:

- **`flight.telemetry`** — `ReplacingMergeTree(observed_at)` over the
  raw events. Re-delivered duplicate `event_id` is collapsed by a
  background merge. Read queries use `FINAL` (via the
  `flight.telemetry_final` view) so callers never see pre-merge
  duplicates. 180-day TTL.
- **`flight.telemetry_hourly_by_country`** — `AggregatingMergeTree`
  materialized from the raw table at write time. One row per
  `(origin_country, hour)` with `uniqExactState(icao24)` plus
  `SimpleAggregateFunction(sum, ...)` for velocity / altitude
  averages, and counts for climbing / descending / cruising /
  on-ground / emergency-squawk events.
- **`flight.telemetry_hourly_by_airline`** — same shape but keyed on
  the first 3 chars of `callsign` (the ICAO airline code). Powers
  per-operator rollups.
- **`flight.airspace_grid_5min`** — geographic density heatmap. 0.25°
  lat/lon cells, 5-minute buckets, airborne aircraft only.
  `uniqExactState(icao24)` per cell. 30-day TTL.

`uniqExactState` instead of `count()` is the load-bearing choice:
re-delivered events would double-count under `count()` until the
ReplacingMergeTree merge kicks in. `uniqExact` stores hash sets, not
integers, so duplicates collapse mathematically at read time.

Why ClickHouse and not, say, DuckDB, Snowflake, or Druid?
- **Self-hostable, free, single-node friendly**. Perfect for local
  dev and still scales linearly across a real cluster.
- **SQL that doesn't lie** to you about dedupe semantics under
  at-least-once ingestion, via the `uniqExact` family.
- **Merge trees compose**. Adding another rollup (e.g. a daily
  summary on top of one of the hourly tables) is just more SQL.

### Iceberg on MinIO — the long-term lake

Role: multi-month or multi-year retention, full-history replay,
ad-hoc data-science queries.

The lake is deliberately boring: **flat Parquet files** laid out in
S3-like object storage (MinIO locally, real S3 in production),
organized by an **Apache Iceberg** table format that tracks which
files belong to which version of the table.

The crucial idea: **Iceberg is not a query engine**. It's a
specification that says "a table is a set of Parquet data files plus
a tree of metadata (snapshots, manifests) describing which files are
part of which version." The metadata lives as small JSON/Avro files
next to the Parquet; a query engine (DuckDB, Trino, Spark, Snowflake
External Tables, etc.) reads the metadata to figure out which files
to scan, and then reads the Parquet directly.

This split buys us:
- **Schema evolution** — add a column on Monday, old Parquet files
  that don't have it still read cleanly (Iceberg supplies defaults).
- **Time travel** — every write creates a snapshot. "Show me the
  table as of last Tuesday" is one SQL clause.
- **Partitioning that's transparent to the caller** — a query with
  `WHERE observed_at BETWEEN ...` automatically prunes Parquet files
  whose day-partition doesn't overlap the range.
- **Zero vendor lock-in**. Any engine that speaks Iceberg reads
  these files; moving off MinIO to real S3 is a DNS change.

**PyIceberg** (the Python Iceberg client) is what our `archiver`
service uses to write to the lake. **DuckDB** is what `lake-query`
uses to read it. Neither of them runs as a long-lived service —
PyIceberg is invoked inside the archiver process; DuckDB is invoked
for a single query then exits. Both are in-process libraries, not
database servers.

Why MinIO locally? S3 costs money and requires network egress. MinIO
is a single container that implements the S3 HTTP API bit-for-bit,
so our `archiver` and `lake-query` code is S3-ready from day one.

### Why not just one store

A single store can't beat this split on raw cost/performance. But
there's a subtler reason: **three stores keep each tool within its
sweet spot**, which means our code is simpler. We never fight
Postgres into doing columnar aggregates, never fight ClickHouse into
being a primary key store, never fight Iceberg into serving
sub-second reads. Each sink writes only what its store was built for.

---

## 6. Observability — OpenTelemetry, Prometheus, Grafana, Jaeger

The stack produces three signals: **logs, metrics, traces**. Each
lands in a different place, and one SDK (OpenTelemetry) emits all
three.

### OpenTelemetry

The vendor-neutral SDK used by every Go service. We initialize it
in `cmd/*/main.go` via the shared `internal/otel` helper. It wires:

- A **TracerProvider** that exports spans via OTLP/HTTP to the
  local collector.
- A **MeterProvider** that exposes Prometheus-format metrics on a
  per-service port (`/metrics`, 9464–9467).
- A **TextMapPropagator** (W3C TraceContext + Baggage) so trace IDs
  flow through HTTP headers *and* through Kafka message headers
  (via a small carrier in `internal/tracing/kafka.go`, because
  `segmentio/kafka-go` has no upstream OTel instrumentation).

We chose OTel because it's the portable contract — swap the
collector config and our code exports to Honeycomb, Datadog, or an
in-cluster Jaeger with no code change.

### Prometheus

Scrapes `/metrics` on each service every 15s. Stores a time-series
database. We currently export only Go runtime metrics (goroutines,
heap, GC, CPU) — business-level counters (events ingested per
status, sink lag, batch latency histograms) are deferred work.

Why Prometheus and not, say, StatsD or InfluxDB? Because the
Kubernetes world standardized on Prometheus, Grafana speaks it
natively, and its query language (PromQL) is the lingua franca for
alerting.

### Grafana

The single pane. One datasource per signal source: Prometheus for
metrics, Jaeger for traces (wired via the Jaeger datasource so you
can click through traces from time-series panels). We have a small
hand-built dashboard (`Dream Flight — Runtime`) showing
per-instance Go runtime state and scrape health.

### Jaeger

Stores distributed traces. When a `POST /events` comes in, a span
tree is created:

```
POST /events                (otelhttp middleware, ingest-api)
  ingest.event              (per-event, inside the batch loop)
    kafka.produce           (injects traceparent into Kafka headers)
      kafka.consume         (extracts traceparent, in stream-processor)
        postgres.insert_event
          pg.insert flight_telemetry
          pg.upsert aircraft_state
```

The W3C TraceContext rides in a Kafka message header, so the trace
from the HTTP request continues across process boundaries into the
consumer, across one more boundary into the Postgres tx. In Jaeger
you see one flame graph for the entire lifetime of an event.

Why Jaeger and not Tempo or Zipkin? Jaeger has the most mature UI,
OTel-native support, and is production-proven. Tempo would be a
better pick in a metrics-heavy Grafana Cloud setup; in a self-hosted
learning project, Jaeger wins.

### What's instrumented today, what isn't

Traces: end-to-end on the `POST /events → Kafka → processor →
Postgres` path. The ClickHouse sink and Iceberg archiver produce
zero spans (instrumenting them is deferred work — the carrier is
already written, it's just a matter of adding `tracer.Start` in
each).

Metrics: only Go runtime + process — no custom app counters yet.
`ingest_events_total`, `sink_lag_seconds`, `request_duration_seconds`
would be the next natural additions.

---

## 7. Local orchestration — docker-compose and kind together

Two orchestrators, each playing to its strength.

### docker-compose — the infra layer

`deploy/docker-compose.yml` owns the **infra**: Kafka, Schema
Registry, Postgres, ClickHouse, MinIO, Iceberg REST catalog, plus
the observability overlay (OTel collector, Prometheus, Grafana,
Jaeger). One `make up` and all of it is healthy and addressable on
`localhost:...` ports.

Compose is the right tool for this because:
- It's faster to spin up a Postgres container than to configure a
  CloudNativePG operator.
- It's fewer moving parts than deploying a Kafka operator (Strimzi)
  locally.
- For developers, compose logs are more ergonomic than
  `kubectl logs -l` with dynamic pod names.

### kind — the application layer

`deploy/kind/kind-cluster.yaml` + `scripts/kind-up.sh` brings up a
**single-node Kubernetes cluster** that runs the six pipeline services
(four Go, two Python) plus a daily Postgres-retention CronJob and an
in-cluster Prometheus + ArgoCD. Service images are pulled from the
local Docker daemon (via `kind load docker-image`), deployed via Helm
charts (`deploy/helm/<service>/`), and reach the compose infra over
`host.docker.internal`.

This split is not production-shaped — in production everything would
live in one Kubernetes cluster — but locally it's the sweet spot:
you learn k8s/Helm/ArgoCD mechanics on just the code you wrote,
without the operational burden of running a database inside kind.

A small but necessary plumbing detail: Kafka advertises a third
listener (`PLAINTEXT_KIND://host.docker.internal:39092`) exclusively
for kind pods, because the existing listener advertises "localhost"
which kind pods can't resolve. Once you see the pattern, it's
trivial.

Why kind and not minikube or Docker Desktop k8s? kind is closest to
"real" Kubernetes — same control plane, same CRI, same CNI shape
— and is fast to create/destroy. Minikube hides more abstraction.
Docker Desktop's built-in k8s is fine but less explicit.

---

## 8. Deployment — Dockerfiles, Helm, and ArgoCD

### Dockerfiles

Every Go service ships with a `Dockerfile` under `cmd/<service>/`.
The pattern is identical across all four:
- Multi-stage build (golang → distroless/static).
- Non-root user (UID 65532).
- Read-only root filesystem, dropped capabilities, seccomp
  RuntimeDefault at the pod level.
- Image size well under 30 MB per service because we stand on
  `gcr.io/distroless/static`.

Why distroless? Smallest possible attack surface — no shell, no
package manager, nothing an attacker could pivot through. `scratch`
would be even smaller but distroless gives us CA certs for TLS.

### Helm charts

`deploy/helm/<chart>/` — one chart per pipeline service plus one
infra chart (`postgres-retention`, see §10). Each service chart has
a single `Deployment` and (for ingest-api and query-api) a `Service`;
the infra chart ships a `CronJob` only. Values are split between
`values.yaml` (defaults that assume in-cluster infra) and
`values-kind.yaml` (the local override that points at
`host.docker.internal`).

Why Helm and not raw `kubectl apply -f`? Because:
- **Template variables**. Every chart needs to vary image tag,
  replica count, env vars per environment. Helm makes that trivial.
- **Release tracking**. `helm list` shows what's deployed; `helm
  rollback` works.
- **It's the universal language**. Every open-source k8s component
  ships a Helm chart. ArgoCD speaks Helm natively.

We specifically avoided a mega-chart that deploys everything at once.
One chart per service means one rollback unit, one changelog, one set
of values — matches the micro-service deploy boundary.

### ArgoCD

ArgoCD runs in the kind cluster and its manifests live at
`deploy/argocd/`:
- `root.yaml` — the "app-of-apps" Application; apply this once and
  ArgoCD picks up every file under `deploy/argocd/apps/`
  automatically.
- `deploy/argocd/apps/<chart>.yaml` — one Application per chart
  (six pipeline services + one CronJob = seven Applications),
  pointing at the Helm chart path in this repo on `main`.

The GitOps loop is: push to `main` → ArgoCD detects the commit
within ~3 min → reconciles the cluster to match. `automated.prune`
removes resources you delete from Git; `automated.selfHeal` undoes
manual kubectl edits that drift from the declared state.

Why ArgoCD and not Flux or plain scripts?
- **Pull-based**: the cluster pulls from Git, so CI doesn't need
  cluster credentials. This is the production-grade security
  posture.
- **First-class UI**: seeing sync status, diffs, and rollback in one
  place is invaluable while learning.
- **Industry standard**: ArgoCD is the most common answer for
  GitOps in the Kubernetes world.

Local access (kind doesn't ship a default ingress): port-forward the
ArgoCD server with `kubectl -n argocd port-forward svc/argo-cd-argocd-server 8443:80`,
then open <http://localhost:8443>. Initial admin password lives in
the `argocd-initial-admin-secret` Secret.

---

## 9. The cross-cutting themes

These aren't in any one service — they're design patterns that
appear in every box in the diagram.

**At-least-once delivery + idempotent sinks.** We never try for
exactly-once across the pipeline because it would require expensive
two-phase commits between Kafka and each sink. Instead we embrace
duplicates at the wire level and make every sink reject them:
- Postgres: `flight_telemetry.event_id` is the primary key + `ON CONFLICT
  DO NOTHING`.
- ClickHouse: `ReplacingMergeTree` keyed on `event_id` + the
  `flight_telemetry_final` view hides duplicates from readers.
- Iceberg: no native uniqueness, so we dedupe at read time via
  `SELECT DISTINCT ON (event_id)` or `COUNT(DISTINCT event_id)`.

**Kafka-offset commits lag downstream writes.** Each consumer's
offset advances *only after* the write to its sink has durably
committed. A crash between DB commit and offset commit replays the
last message; idempotence absorbs it. This is the whole reason the
Kafka-offset invariant (I3 in the README) holds.

**Kafka partition key = ordering lane.** By keying on
`icao24`, every event for a given entity lands in the
same partition and is consumed by a single consumer instance in
strict order. Scaling consumers horizontally (up to the partition
count) keeps per-entity ordering intact — this is what makes the
system safe to scale.

**Shutdown is a correctness problem, not a convenience.** Every
service uses `signal.NotifyContext` + a detached 5-second context
for the final Kafka offset commit, so a SIGTERM doesn't orphan the
last in-flight message. HTTP servers drain via `Shutdown(10s)`.
Without this, a rolling deploy would drop events.

**Security posture is layered, not absolute.** Distroless images,
non-root users, read-only root filesystems, dropped capabilities.
No ingress auth yet (ingest-api/query-api would sit behind an API
gateway). No TLS between services yet (in production mTLS via
Linkerd/Istio or a mesh). The point isn't "we've solved security" —
it's "we've taken the free 90% and clearly flagged the paid 10%."

---

## 10. Retention and what's deliberately half-built

### Retention is layered across the three stores

Each store carries its own retention policy, sized to its role:

- **Postgres `flight_telemetry`** — pruned by a daily K8s CronJob
  (`deploy/helm/postgres-retention/`) that runs at 03:00 UTC and
  DELETEs rows older than 7 days. Postgres has no native TTL, so
  this fills the gap. `aircraft_state` is intentionally *not*
  pruned — it's the latest-known-state cache, naturally bounded to
  the ~10k aircraft active globally at any moment.
- **ClickHouse `flight.telemetry`** — 180-day TTL declared in the
  table DDL; the MergeTree engine drops expired rows during normal
  background merges. Hourly rollups are TTL'd at 90 days, the
  airspace grid at 30 days.
- **Iceberg `flight.telemetry`** — no TTL. The lake is the
  long-term source of truth.
- **Kafka topic** — 48-hour log retention (compose env). Sinks have
  always absorbed older events into one of the three stores by
  the time Kafka prunes.

### What a full production version would still add

- **Real auth on `ingest-api` and `query-api`** — JWT or API key at
  an ingress layer.
- **Kafka: RF=3, 3 brokers, idempotent producer, SASL + TLS**.
- **Application-level metrics** — per-endpoint latency histograms,
  per-sink lag gauges, per-partition consumer health.
- **Traces on `clickhouse-sink` and `archiver`** — the carrier is
  already there, just need `tracer.Start` calls.
- **Schema compatibility CI** — a GitHub Action that fails on
  incompatible Avro changes before they hit main.
- **Postgres replication + read replica** for `query-api`.
- **Iceberg compaction** — over time the lake accumulates small
  Parquet files; a scheduled job should coalesce them.
- **Secrets management** — right now Postgres DSNs are in values
  files; in production they'd come from Vault / Secret Manager /
  Sealed Secrets.

Each of those is a focused future project; none is blocking the
core pipeline from working end-to-end today.

---

## 11. How to read the code

If you want to understand:

- **"How does an event actually get from HTTP to Postgres?"** →
  read `internal/api/handler.go`, then
  `internal/producer/producer.go`, then `internal/processor/processor.go`,
  then `internal/processor/store.go`. That's the four hops end to end.
- **"How does the Iceberg writer work?"** →
  `services/archiver/archiver.py` is ~300 lines and contains the
  whole thing.
- **"How is the pipeline tested?"** → unit tests live next to each
  package (`_test.go`). Integration tests with Postgres via
  `testcontainers-go` are in `internal/processor/store_test.go`
  (build tag `integration`).
- **"How does the observability wiring actually work?"** →
  `internal/otel/otel.go` is the SDK init; `internal/tracing/kafka.go`
  is the header carrier; the spans are created where you'd expect
  (HTTP handler, producer, consumer, store).
- **"How is this deployed?"** → `deploy/docker-compose.yml` for
  infra, `deploy/helm/*/` for the services, `deploy/argocd/` for the
  GitOps layer.

Everything else — the Python CLI tools, the scripts, the chart
templates — is small and isolated enough to read in one sitting.
