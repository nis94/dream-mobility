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

## Compose port bindings are loopback-only

All host-side compose port mappings are prefixed with `127.0.0.1:` so
dev services are unreachable from the LAN. kind pods still reach them
via `host.docker.internal`, which Docker Desktop resolves to the Mac's
loopback interface. To enable anonymous MinIO read on the lake bucket
(convenient for poking Iceberg files from the browser), set
`MINIO_ANONYMOUS=1 make up` — default is off.

## Python tools need S3 creds explicitly

- `services/archiver/archiver.py` and `tools/lake-query/query.py`
  **refuse to start without** `S3_ACCESS_KEY` / `S3_SECRET_KEY`. There
  is no local default on purpose (guards against a fat-fingered
  invocation reaching a non-local lake with `minioadmin`). For local
  dev: `S3_ACCESS_KEY=minioadmin S3_SECRET_KEY=minioadmin`.

## Kafka partitioning dictates scale-out

- Topic `flight.telemetry` has **3 partitions**. Per consumer group, at
  most 3 instances consume in parallel; a 4th sits idle with
  `#PARTITIONS = 0` (verify with
  `kafka-consumer-groups --describe --group <g> --members`).
- The three sinks (`flight-postgres`, `flight-clickhouse`,
  `flight-iceberg`) are **independent consumer groups** and scale
  independently.
- Per-entity ordering (invariant I6) holds under scale-out because the
  producer keys on `icao24` — same key → same partition
  → single consumer.

## Kubernetes / GitOps layout

- The **compose stack owns infra**; the **kind cluster owns the four Go
  services** (ingest-api, stream-processor, query-api, clickhouse-sink).
  Kind pods reach compose infra via `host.docker.internal`. See
  `deploy/kind/kind-cluster.yaml` for node port mappings
  (30080→host:8080, 30443→host:8443).
- Kafka has **three** listeners: 9092 for the docker network, 29092
  (advertised as `localhost`) for host-side Python tools, 39092
  (advertised as `host.docker.internal`) for kind pods. One listener
  can only advertise one name, so all three are necessary.
- Helm values are split: `values.yaml` holds production-ish defaults
  (in-cluster DNS like `postgres:5432`); `values-kind.yaml` overrides
  them with `host.docker.internal` for the kind case.
- `scripts/kind-up.sh` + `scripts/kind-deploy.sh` bootstrap the whole
  local k8s setup; `scripts/kind-deploy.sh` is the imperative path
  (direct `helm install`). **ArgoCD is the declarative path** and
  supersedes it in steady state.
- **ArgoCD is installed in the `argocd` namespace**; UI at
  `https://localhost:8443` (NodePort 30443), admin password is in
  the `argocd-initial-admin-secret` Secret.
- `deploy/argocd/root.yaml` is the app-of-apps entry point —
  `kubectl apply -f` once and ArgoCD picks up every file under
  `deploy/argocd/apps/` automatically. `automated.prune + selfHeal`
  are on, so manual `kubectl edit` drifts are reverted.

## Observability stack caveats

- Opt-in overlay compose:
  `docker compose -f deploy/docker-compose.yml
  -f deploy/observability/docker-compose.observability.yml -p dream-flight up -d`.
- OTel SDK is initialized in every `cmd/*/main.go`. The OTLP/HTTP
  exporter **must** use `otlptracehttp.WithInsecure()` against the
  local collector (the SDK defaults to HTTPS otherwise and every
  export silently fails with `server gave HTTP response to HTTPS
  client`).
- **Custom metrics**: still none. `/metrics` is Go runtime + process
  only. Adding ingest/sink counters is deferred work.
- **Traces**: instrumented end-to-end across Go AND Python services.
  The Go ingest-api → Kafka → stream-processor → Postgres path uses
  W3C TraceContext carried on Kafka message headers via the carrier
  in `internal/tracing/kafka.go`. The Python archiver extracts the
  same `traceparent` via `opentelemetry.propagate.extract()` and
  continues the trace into Iceberg writes — one trace ID now spans
  Go→Go→Python. clickhouse-sink is not yet instrumented.
- Prometheus is reachable from Grafana at
  `http://df-prometheus:9090` (the docker-network hostname), not
  `localhost`. Jaeger is reachable the same way at
  `http://df-jaeger:16686`. Datasources are provisioned by hand
  today (`curl -u admin:admin -X POST /api/datasources`).
- Grafana runs with anonymous Viewer + `admin/admin` for edit. Dev-only.
- `deploy/observability/prometheus.yml` statically scrapes three
  `stream-processor` ports (9466/9476/9486) so three local replicas
  are visible. This is a local-dev hack — in k8s swap for a
  `PodMonitor` label selector.
