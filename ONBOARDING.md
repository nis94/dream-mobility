# Onboarding — Dream Flight

A flow-oriented reading plan for a developer joining this repo cold.
Follow it top to bottom; each step tells you exactly which file(s)
to open and what to look for. Budget: **one focused day**. By the
end you should be able to trace any single event from `POST /events`
(or OpenSky) through every store, and know which file owns each hop.

This file complements (does not replace):

- [README.md](./README.md) — operational reference (make targets, env
  vars, endpoints, invariants).
- [OVERVIEW.md](./OVERVIEW.md) — narrative tour of design choices.
- [CLAUDE.md](./CLAUDE.md) — sharp-edges that will trip you up
  (port reuse, OpenSky rate limit, kind/compose plumbing).

If you only have 30 minutes, do **§1 (Day 0)** and **§2 (Mental model)**.
That alone tells you what the system does and how to poke it.

---

## 0. Prereqs — what to refresh before reading code

You don't need to be expert in any of these, but knowing the
vocabulary will save you hours staring at code wondering *why* a
choice was made. One paragraph per topic, in the order they appear
in the pipeline.

### Go (the four hot-path services are Go)

- **chi** — minimalist HTTP router used by `ingest-api` and
  `query-api`. Idiomatic stdlib-shaped middleware chain.
- **pgx / pgxpool** — high-perf Postgres driver. We use the pool +
  raw SQL, no ORM. Note: `pgxpool.ParseConfig` respects `pool_*` DSN
  params, our code only fills in defaults if those weren't set.
- **segmentio/kafka-go** — the Kafka client. Manual offset commit
  via `FetchMessage` + `CommitMessages`. No upstream OTel
  instrumentation, hence our small carrier in
  `internal/tracing/kafka.go`.
- **`signal.NotifyContext`** — every `cmd/*/main.go` uses this for
  graceful shutdown. The pattern is: build a `ctx`, run the service,
  on signal cancel everything; final Kafka commits use a *detached*
  5-second context so SIGTERM doesn't orphan offsets.

### Kafka concepts you must hold in your head

- **Topic = partitioned log.** Our topic `flight.telemetry` has
  **3 partitions**. Producer keys by `icao24` so all events for one
  aircraft hit the same partition → in-order per aircraft.
- **Consumer group = independent reader cursor.** Each of our three
  sinks (`flight-postgres`, `flight-clickhouse`, `flight-iceberg`) is
  a *separate group*; replaying one does not affect the others.
- **Offset commit lags the write**, never the other way around. The
  whole at-least-once design depends on this.
- **One partition is owned by exactly one consumer per group.** A
  4th replica in a 3-partition topic just sits idle.

### Avro + Confluent Schema Registry wire format

- Every Kafka message value is `0x00` (magic byte) + 4-byte
  big-endian schema ID + Avro binary. Producers register schemas
  once; consumers strip the prefix, look up the schema by ID, decode.
- See `internal/producer/producer.go:137-143` (`encodeWireFormat`)
  and `internal/processor/decode.go` for the two ends of this.

### Postgres dedupe + timestamp-gated upsert

- `INSERT ... ON CONFLICT (event_id) DO NOTHING` makes the raw insert
  idempotent.
- `INSERT ... ON CONFLICT (icao24) DO UPDATE ... WHERE EXCLUDED.observed_at > aircraft_state.observed_at`
  means an out-of-order replay does **not** regress the
  last-known position. The `>` is strict — equal timestamps lose.

### ClickHouse merge-tree engines

- **`ReplacingMergeTree(version)`** — duplicates by `ORDER BY` key
  collapse during background merges, keeping the row with the highest
  `version`. We use `observed_at` as the version. Read with the
  `*_final` view (which uses `FINAL`) to never see un-merged dupes.
- **`AggregatingMergeTree`** stores partial-aggregate states (e.g.
  `uniqExactState(icao24)`) and merges them on background or at read
  time via `uniqExactMerge`. This is why **redelivered duplicates do
  not double-count** the way they would in a `SummingMergeTree`.
- **Materialized views** are write-time triggers in ClickHouse, not
  cached read-time queries. The MV runs on each `INSERT` block.

### Iceberg vs Parquet vs MinIO (do not conflate)

- **MinIO** is the storage — an S3-API object store running locally.
- **Parquet** is the file format the data lives in.
- **Iceberg** is the *table format*: it tracks which Parquet files
  belong to which version of a logical table, plus partition specs and
  schema. It is metadata, not a query engine.
- A **query engine** (DuckDB, Trino, Spark) reads Iceberg metadata
  to figure out which Parquet files to scan.
- Field IDs in the Iceberg schema are immutable forever. Renaming a
  field changes its name only; queries by ID still resolve.

### OpenTelemetry — three signals from one SDK

- We initialize the SDK once per Go service in `internal/otel/otel.go`
  (called from each `cmd/*/main.go`).
- **Traces** flow over OTLP/HTTP to a local collector. Today the
  collector exports them only to its `debug` exporter (stdout) —
  visible via `docker logs df-otel-collector`. Adding a Tempo/Jaeger
  exporter back to `deploy/observability/otel-collector.yaml`
  restores a trace UI.
  W3C TraceContext rides on **HTTP headers** (otelhttp middleware on
  ingest-api) and on **Kafka message headers** (our carrier in
  `internal/tracing/kafka.go`). Same trace ID flows Go → Go → Python.
- **Metrics** are exposed Prometheus-style on a per-service port
  (9464 / 9465 / 9466 / 9467). Today we export Go runtime only —
  no business counters yet.
- **Logs** are plain `log/slog` JSON to stdout.

### Docker Compose vs kind + Helm + ArgoCD

- **Compose owns the infra** (Kafka, SR, Postgres, ClickHouse, MinIO,
  Iceberg REST). One `make up`.
- **kind owns the application** (six Helm charts, one per service)
  + ArgoCD for GitOps. Kind pods reach compose infra via
  `host.docker.internal` — that's why the Kafka broker advertises a
  *third* listener on port 39092 just for kind.

If something on this list is unfamiliar, lean on the OVERVIEW for the
"why" and on the file walks below for the "how."

---

## 1. Day 0 — get the system running and watch one event flow

Do this first. Watching a real event move makes every later read
ten times faster.

```bash
# 1. Bring up the infra (Kafka, SR, Postgres, ClickHouse, MinIO, Iceberg REST)
make up

# 2. Register the canonical Avro schema with Schema Registry
make register-schemas

# 3. Run the four Go services on the host (easiest for first pass)
go run ./cmd/ingest-api       &
go run ./cmd/stream-processor &
go run ./cmd/query-api        &
go run ./cmd/clickhouse-sink  &

# 4. Push a synthetic burst with duplicates + out-of-order events
make generator-install
cd tools/generator && uv run python gen.py \
  --rate 50 --duration 10 --entities 5 \
  --duplicates 0.20 --out-of-order 0.15 \
  --target http://localhost:8080/events:batch

# 5. Verify the event made it into the hot store
curl -s 'http://localhost:8090/flights/active' | jq '. | length'
curl -s 'http://localhost:8090/aircraft/abc123' | jq

# 6. Verify it made it into ClickHouse too
docker compose -f deploy/docker-compose.yml -p dream-flight \
  exec clickhouse clickhouse-client -q \
  "SELECT count(), uniqExact(event_id) FROM flight.telemetry_final"

# 7. Done — tear down when finished
make down       # keep volumes
make down-v     # destructive (wipe data)
```

Open the **Kafka UI** (<http://localhost:8088>) while you do this.
Watch messages land on `flight.telemetry`, then watch the three
consumer groups (`flight-postgres`, `flight-clickhouse`,
`flight-iceberg`) advance their offsets. This is the system.

If you turned on the observability overlay
(`docker compose -f deploy/docker-compose.yml -f deploy/observability/docker-compose.observability.yml up -d`),
the collector logs each span to stdout — `docker logs df-otel-collector`
will show the spans for one event in chronological order: HTTP handler
→ producer → Kafka header → consumer → Postgres transaction. Same
shape as Jaeger's flame graph, just rendered as JSON instead of UI.
Re-enable a trace UI by adding an exporter back to
`deploy/observability/otel-collector.yaml`.

---

## 2. The mental model (read these two things, in this order)

1. [README.md](./README.md) — only the **Architecture** and **End-to-end
   data flow** mermaid diagrams. ~5 minutes.
2. [OVERVIEW.md](./OVERVIEW.md) §1 (the problem) and §2 (the shape of
   the answer). ~5 minutes.

After this you should be able to draw, on a whiteboard, the boxes
and arrows. The single sentence that summarizes everything is:
**"Write once into Kafka, read three ways out of three sinks; every
sink is idempotent on `event_id`."** Hold that, the rest is details.

---

## 3. Walk the data plane — file by file, in flow order

For each step: open the file, read the named function/anchor, ask
yourself the listed question. Total reading: ~3 hours.

### 3.1 Where events come from (the two sources)

There are two sources that produce to the same Kafka topic:

- **Live data**: `services/opensky-ingest/opensky_ingest.py` polls
  OpenSky `/states/all` every 300 seconds and produces direct to
  Kafka (skips ingest-api on purpose — pull-based source has no
  reason to round-trip through HTTP). **Don't drop the poll interval
  below ~215s without OAuth2** (see `CLAUDE.md` for the rate-limit
  footgun).
- **Push clients (and chaos testing)**: `tools/generator/gen.py`
  posts batches to `ingest-api`'s `POST /events`. Use
  `--duplicates` and `--out-of-order` to deliberately exercise the
  dedupe and timestamp-gate paths.

Open both files briefly. Both are small (~400 lines and ~one file
each). The Python services are deliberately thin — they're
adapters, not business logic.

### 3.2 The wire contract — Avro schema

Read **`internal/avro/flight_telemetry.avsc`** (the source of truth)
and **`internal/avro/flight_telemetry.go`** (the generated struct).

Look for: 17 typed fields, one-to-one with OpenSky's `/states/all`
row shape. `event_id` is `logicalType=uuid` (the dedupe key);
`observed_at` is `timestamp-micros` (the version key for out-of-order
gating). Nullable fields use the union `[null, T]` Avro idiom.

`gen.go` shows the codegen invocation (`go generate ./...` invokes
`avrogen`). The generated struct's `Marshal()` does the binary
encoding; see how the producer uses it next.

### 3.3 Push entry point — `ingest-api`

Open these two files in order:

1. **`cmd/ingest-api/main.go`** (127 lines) — the wiring story.
   Note the order: parse config → install signal handler → init OTel
   → create Producer → wrap handler with `otelhttp.NewHandler` →
   start HTTP server → block on signal → graceful shutdown. Every
   Go service follows this same pattern; learn it once.

2. **`internal/api/handler.go`** — the HTTP handler.
   - `Ingest` (line 54): accepts three body shapes (single event,
     batch wrapper, raw array). Body capped at 10 MiB.
   - `processOne` (line 89): per-event span (so one trace = one
     event), validate → map to Avro → `producer.Produce`. Errors
     accumulate into `IngestResponse`; the response is `202` if
     anything succeeded, else `400`.
   - `mapToAvro` (line 197): one-to-one field copy; ts is already
     parsed by `ValidateEvent`.

Also skim `internal/api/validation.go` (91 lines) — it's the
defensive boundary: lat/lon ranges, callsign length, ICAO24 format.

### 3.4 Producer + SR wire format

**`internal/producer/producer.go`** (250 lines, the most subtle
file in the repo).

- `New` (line 53): registers the canonical schema *embedded* from
  `internal/avro/flight_telemetry.avsc` (do not strip it — see
  Phase 2 audit note in README, this caused a real bug).
- The `Murmur2Balancer{Consistent: true}` is the hash that matches
  Confluent default. Anyone else writing to this topic from Java/
  librdkafka must use the same hash to preserve per-key ordering.
- `Produce` (line 87): keys on `event.Icao24`, marshals Avro, prepends
  the SR prefix in `encodeWireFormat` (line 137), injects W3C
  TraceContext into Kafka headers via the carrier.
- `registerSchema` (line 159): retries on transient failures
  (network, 408, 429, 5xx) with bounded linear backoff. Hard-fails on
  4xx.

After reading this, you should understand exactly what's on the wire
on the producer side and **where** the trace context starts riding
along.

### 3.5 Kafka — partitioning, ordering, fan-out

There is no code in this repo for "Kafka" beyond config — the
broker is a compose container. But understand:

- Topic `flight.telemetry`, **3 partitions**, key = `icao24`.
- Three consumer groups, each independent: `flight-postgres`,
  `flight-clickhouse`, `flight-iceberg`. Run `make up` then in
  another shell:

  ```bash
  docker compose -f deploy/docker-compose.yml -p dream-flight \
    exec kafka kafka-consumer-groups \
    --bootstrap-server localhost:9092 --list
  ```

- 48-hour log retention (compose env). Sinks must keep up; if any
  sink falls behind by 48h it loses events.
- Three listener ports: 9092 (docker network), 29092 (host tools),
  39092 (kind pods via `host.docker.internal`). One listener can
  only advertise one hostname — that's why three.

### 3.6 stream-processor → Postgres (sink #1)

This is the most tightly-correctness-checked path; spend the most
time here. Read in this order:

1. **`cmd/stream-processor/main.go`** (90 lines) — wiring; same
   shape as ingest-api's main.

2. **`internal/processor/processor.go`** (224 lines) — the consumer
   loop.
   - `New` (line 49): explicit `CommitInterval: 0` — no auto-commit.
     This is load-bearing for I3.
   - `Run` (line 73): the loop. Note the **detached commit context**
     at line 110 — using the request `ctx` here would silently
     re-deliver the last message on SIGTERM.
   - `processMessage` (line 119): extracts the W3C TraceContext from
     Kafka headers (continuing the producer's trace), decodes,
     inserts. Decode failures are committed-past (poison messages
     would otherwise wedge the consumer).
   - `kafkaLogger.Printf` (line 219): filters out the noisy
     "no messages received within allocated time" idle heartbeat.

3. **`internal/processor/decode.go`** (41 lines) — strip the SR
   wire-format prefix, unmarshal Avro. Mirror image of
   `producer.encodeWireFormat`.

4. **`internal/processor/store.go`** (190 lines) — the Postgres
   write path.
   - `NewStore` (line 39): pool sizing (override-able via DSN
     `pool_*` params), runs the embedded `schema.sql` at startup as
     migration.
   - `InsertEvent` (line 96): **the entire correctness story in one
     transaction**. Two SQL statements: the dedupe insert
     (`ON CONFLICT (event_id) DO NOTHING`) and the timestamp-gated
     state upsert (`WHERE EXCLUDED.observed_at > aircraft_state.observed_at`).
     Read both statements *carefully* — they encode invariants I1
     and I2.
   - The `defer tx.Rollback(context.Background())` at line 117 uses
     a fresh context for the same reason the commit does.

5. **`internal/processor/schema.sql`** — the table DDL.
   Two tables: `flight_telemetry` (one row per observation, PK on
   `event_id`) and `aircraft_state` (one row per aircraft, PK on
   `icao24`). Indexes are tuned for the query-api access patterns
   (`(icao24, observed_at DESC)` and
   `(origin_country, observed_at DESC)`).

6. **`internal/processor/store_test.go`** (build tag `integration`)
   — uses `testcontainers-go` to spin up a real Postgres. Reading
   the test names is a great way to confirm what the contract is.

### 3.7 clickhouse-sink → ClickHouse (sink #2)

ClickHouse hates single-row inserts; this sink is **batched**. The
batching + offset story is the whole subtlety — read in this order:

1. **`cmd/clickhouse-sink/main.go`** (311 lines, longest of the four
   mains because it owns the consumer loop directly). Look for the
   batch-flush trigger (size-based `Len() >= batchSize` or
   timer-based `BatchTimeout`). **The consumer loop, not the sink,
   commits offsets after each successful flush.** This is the Phase 5
   audit fix; read the package doc-comment in
   `internal/chsink/sink.go:1-10` for the rationale.

2. **`internal/chsink/sink.go`** (257 lines) — pure buffer +
   batch-write. No goroutines, no offset awareness.
   - `New` (line 68): two-phase bootstrap (create DB, then connect to
     it) to avoid a chicken-and-egg with `Ping()`.
   - `Add` / `Len` / `Flush` / `Close` — note `Flush` copies the
     buffer before releasing the lock so `writeBatch` can't race
     a concurrent `Add`.
   - `writeBatch` (line 185): `PrepareBatch` → `Append` per row →
     `Send`. The batch is one INSERT under the hood.

3. **`internal/chsink/schema.sql`** — the most interesting SQL in the
   repo. One raw `ReplacingMergeTree(observed_at)` table plus three
   `AggregatingMergeTree` rollups (by country, by airline, by
   airspace grid), each fed by a `MATERIALIZED VIEW`. The
   `uniqExactState(icao24)` columns are the load-bearing detail —
   they're what makes counts dedupe-safe under at-least-once delivery.

After reading you should understand: why batching, why
`uniqExactState` instead of `count()`, and why the consumer loop
(not the sink) owns offset commits.

### 3.8 archiver → Iceberg (sink #3, Python)

**`services/archiver/archiver.py`** is ~526 lines and contains the
whole Iceberg path. Read top to bottom — it's the single
self-contained story.

Anchors:
- `ARROW_SCHEMA` (line 94) and `ICEBERG_SCHEMA` (line 118) — the
  **same** field order, with explicit Iceberg field IDs 1–18.
  Field IDs are forever; renaming a field changes its name only,
  and queries by ID still resolve.
- `IcebergArchiver._ensure_table` (line 250) — partition spec
  `(days(observed_at), identity(origin_country))`; sort order
  `observed_at ASC` within partition for tight Parquet row-group
  pruning.
- `flush` (line 303) — in-batch dedup by `event_id` (Iceberg has no
  PK). Cross-batch dups are still possible; readers handle them via
  `SELECT DISTINCT ON (event_id)` — see `tools/lake-query/query.py`.
- `run` loop (line 334) — note `enable.auto.commit: False`, the
  size/timer flush-then-commit shape, and the trace continuation via
  `_extract_parent_context` at line 82. **This is where the same
  trace ID that started in the Go ingest-api becomes a Python span.**
- `_safe_commit` (line 461) — defensive offset commit.
- `S3_ACCESS_KEY` / `S3_SECRET_KEY` have **no defaults** — the
  service refuses to start without them. Local dev: `minioadmin /
  minioadmin`. See `CLAUDE.md` for the reasoning.

### 3.9 Read paths — `query-api` and `lake-query`

Read paths consume what the sinks wrote. Two of them.

**Hot reads (Postgres) — `query-api`.**

1. **`cmd/query-api/main.go`** (125 lines) — wiring; same shape as
   ingest-api.
2. **`internal/query/handler.go`** (138 lines) — three endpoints:
   `GET /aircraft/{icao24}`, `/aircraft/{icao24}/track`,
   `/flights/active`. Cursor and limit parsing, error mapping. Note
   the `dataStore` interface is consumer-side — the test injects a
   fake.
3. **`internal/query/store.go`** (345 lines) — the SQL.
   - Pool sizing tuned for read concurrency
     (`MaxConns=20` vs writer's 10).
   - Per-query timeouts: `pointReadTimeout = 5s`,
     `listQueryTimeout = 10s`.
   - `activeWindow = 5*time.Minute` — the "active aircraft" cutoff.
     **Wider than OpenSky's 300s poll interval on purpose**, so an
     aircraft that pings exactly at the boundary doesn't flap in/out.
   - Look for the keyset-pagination SQL — row-wise comparison
     `(observed_at, event_id) < (?, ?)` (NOT just `< observed_at`)
     so two rows with the same microsecond don't collide across
     pages. This is invariant I5.
4. **`internal/query/cursor.go`** (61 lines) — opaque base64
   `(observed_at, event_id)` encode/decode.

**Lake reads — `tools/lake-query/query.py`.**

A one-shot DuckDB-over-Iceberg CLI. Reads the Iceberg metadata,
runs a query against the Parquet files, returns rows. Useful for
ad-hoc historical queries; not a long-running service. Refuses to
start without `S3_ACCESS_KEY` / `S3_SECRET_KEY` — same guardrail
reason as the archiver.

---

## 4. Cross-cutting — read these after you've walked the data plane

### 4.1 Configuration

**`internal/config/config.go`** (129 lines). Plain struct + env
parsing — no Viper, no Cobra, no magic. `RedactDSN` is the
non-obvious bit: every log line that mentions a Postgres DSN passes
through it so the password doesn't leak. Use it any time you log a DSN.

### 4.2 OpenTelemetry init

**`internal/otel/otel.go`** (154 lines). One `Init` function that
wires:
- TracerProvider with OTLP/HTTP exporter (note
  `otlptracehttp.WithInsecure()` — without it the SDK defaults to
  HTTPS and every export silently fails; see `CLAUDE.md`).
- MeterProvider exposing Prometheus on `PROM_PORT`.
- W3C TraceContext + Baggage propagator.

Returns a `shutdown` callback that every `cmd/*/main.go` calls in
its defer chain.

### 4.3 Trace propagation across Kafka

**`internal/tracing/kafka.go`** (45 lines). A 30-line carrier that
implements OTel's `TextMapCarrier` interface against
`[]kafka.Header`. Producer injects via
`otel.GetTextMapPropagator().Inject(ctx, &carrier)`; consumer
extracts via `Extract`. This is the entire reason traces span
process boundaries through Kafka. The Python archiver does the same
thing in `_extract_parent_context` (`services/archiver/archiver.py:82`).

### 4.4 Graceful shutdown — the pattern repeated everywhere

Every `cmd/*/main.go` follows this exact sequence:

```go
ctx, stop := signal.NotifyContext(context.Background(),
    os.Interrupt, syscall.SIGTERM)
defer stop()
// ... start everything wired to ctx ...
<-ctx.Done()
// ... defer chain runs in reverse: HTTP shutdown(10s), Kafka close,
//     OTel shutdown(5s), pool.Close()
```

The single most important detail: **the final Kafka offset commit
uses a *detached* `context.Background()` with a 5s timeout**, not
the request context — otherwise SIGTERM cancels the commit and the
next start replays the last message. Look for `commitTimeout` in
`internal/processor/processor.go:25` and the same pattern in
`cmd/clickhouse-sink/main.go`.

---

## 5. Deployment — how this actually runs locally and beyond

### 5.1 Compose for infra

**`deploy/docker-compose.yml`** is the source of truth. All host
ports are bound to `127.0.0.1` (loopback only) — see CLAUDE.md.
`MINIO_ANONYMOUS=1 make up` opens up MinIO read for browser poking.

### 5.2 kind for services

- **`deploy/kind/kind-cluster.yaml`** — single-node cluster, NodePort
  mappings 30080→8080 (ingest-api) and 30443→8443 (ArgoCD UI).
- **`scripts/kind-up.sh`** + **`scripts/kind-deploy.sh`** — bootstrap.
  `kind-deploy.sh` is the imperative path (direct `helm install`);
  ArgoCD supersedes it once running.

### 5.3 Helm

**`deploy/helm/<chart>/`** — six service charts + one infra chart
(`postgres-retention`, a daily CronJob that deletes
`flight_telemetry` rows older than 7 days). Values are split:
`values.yaml` is production-ish (in-cluster DNS); `values-kind.yaml`
overrides hosts to `host.docker.internal` for the kind case.

Pod security context is consistent across all charts:
non-root UID 65532, read-only root FS, dropped capabilities,
seccomp RuntimeDefault. Production secret pattern: `postgresSecret`
value — DSN comes from a `secretKeyRef`, not the values file.

### 5.4 ArgoCD app-of-apps

**`deploy/argocd/root.yaml`** — apply this once. ArgoCD then picks
up every `deploy/argocd/apps/*.yaml` automatically. `automated.prune`
+ `automated.selfHeal` are on, so manual `kubectl edit` drifts are
reverted.

UI: `https://localhost:8443` (NodePort 30443). Initial admin
password is in `Secret/argocd-initial-admin-secret` in the `argocd`
namespace.

---

## 6. Testing — what runs where

```bash
make test              # unit tests, race-enabled
make test-integration  # tags=integration; testcontainers-go (needs Docker)
make smoke             # end-to-end against the running stack
make sr-state          # dump Schema Registry state
make verify            # build + vet + race tests + compose config syntax
```

- **Unit tests** live next to each package (`*_test.go`). Read
  `internal/processor/processor_test.go`,
  `internal/query/cursor_test.go` (compact illustration of I5),
  `internal/api/validation_test.go`.
- **Integration tests** are gated by the `integration` build tag
  and use `testcontainers-go` to spin a real Postgres (see
  `internal/processor/store_test.go`). Slow but verify actual SQL.
- **Load + chaos**: `loadtest/` — k6 scripts. Read
  `loadtest/README.md` for the four scenarios (ramp, soak, dedupe,
  out-of-order).

---

## 7. Hands-on exercises (verify you got it)

If you can do these without re-reading code, you understand the
system. Estimated 1-2 hours total.

1. **Trace one event end-to-end.** With observability up, push one
   `POST /events` with `curl`, then `docker logs df-otel-collector`
   and find the chronological span list for that event. Identify
   exactly where the trace context is transferred between processes
   (HTTP → Kafka headers → Postgres tx).
2. **Cause a duplicate, prove dedupe works.** Submit the same
   `event_id` twice via `gen.py --duplicates 1.0 --rate 1
   --duration 1`. Verify Postgres has one row, ClickHouse `_final`
   view has one row, Iceberg in-batch dedup logged a drop.
3. **Cause an out-of-order event, prove the gate works.** Submit
   two events with the same `icao24` where the second has an
   *older* `observed_at`. Verify `aircraft_state.observed_at` is
   the newer one (I2).
4. **Find the file that owns invariant I3** (offset advances only
   after DB commit). Hint: it is one specific function and one
   specific decision about which `context.Context` to pass.
5. **Add a new app-level metric.** E.g. a counter
   `ingest_events_accepted_total` in `internal/api/handler.go`.
   Verify it shows up on `http://localhost:9464/metrics`. (No need
   to PR it — it's just to confirm you can wire OTel custom
   instruments.)
6. **Spin up a 4th replica of `stream-processor`** and confirm via
   `kafka-consumer-groups --describe --members` that it sits idle
   with `#PARTITIONS = 0`. This proves you understand the
   partition/consumer math.

---

## 8. Where to look when…

| Question | Start here |
|---|---|
| How does `POST /events` actually work? | `cmd/ingest-api/main.go` → `internal/api/handler.go:54` |
| Where is the SR wire format encoded? | `internal/producer/producer.go:137` |
| What does the Kafka key look like, and why? | `internal/producer/producer.go:88` (key = `icao24`) |
| How does dedupe work in Postgres? | `internal/processor/store.go:121` (`ON CONFLICT (event_id)`) |
| How is out-of-order handled in Postgres? | `internal/processor/store.go:165` (`WHERE EXCLUDED.observed_at > ...`) |
| Why doesn't a SIGTERM lose the last message? | `internal/processor/processor.go:110` (detached commit ctx) |
| How do ClickHouse rollups stay correct under retries? | `internal/chsink/schema.sql` (`uniqExactState`) |
| How does the Iceberg writer not duplicate? | `services/archiver/archiver.py:303` (in-batch dedup, plus read-time) |
| Why three Kafka listeners? | `deploy/docker-compose.yml` + `CLAUDE.md` (one listener can advertise one hostname) |
| How do traces flow across Kafka? | `internal/tracing/kafka.go` + `services/archiver/archiver.py:82` |
| How is the system deployed in production-shape? | `deploy/argocd/root.yaml` (app-of-apps) |
| What can wedge the OpenSky producer? | `CLAUDE.md` "OpenSky rate limit footgun" |
| What do all the `PROM_PORT`s mean? | `CLAUDE.md` "Running services locally" |
| Where are the audit reviews? | `.claude/agents/` and the README "Phases" table |

---

## 9. Things that are deliberately not done yet

These are not bugs — they're flagged future work. Knowing what
isn't there saves you "is this missing or am I missing it?" loops:

- **No auth on `ingest-api` / `query-api`.** Expected to sit behind
  an API gateway.
- **Custom app metrics** (per-endpoint latency, sink lag, batch
  histograms). Today `/metrics` is Go runtime + process only.
- **Traces on `clickhouse-sink`.** Carrier exists, just no
  `tracer.Start` calls yet.
- **Kafka RF=1, single broker.** Dev-only. Production needs RF≥3 +
  SASL/mTLS + idempotent producer.
- **Iceberg compaction.** Small Parquet files accumulate; needs a
  scheduled `rewrite_data_files` job.
- **OAuth2 on OpenSky** to lift the 400-credit/day cap to 4000.
  The TODO is in `services/opensky-ingest/opensky_ingest.py`.

See README "Known limits (deferred)" and OVERVIEW §10 for the full
list.

---

You're done. If you walked everything in §3 and §4 and did at
least exercises 1–3 in §7, you can confidently navigate, modify,
and review code in this repo. Welcome aboard.
