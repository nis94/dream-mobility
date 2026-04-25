#!/usr/bin/env bash
# Builds Docker images for the Go services + the Python services (archiver,
# opensky-ingest), loads them into the "dream-flight" kind cluster, and
# installs (or upgrades) the Helm charts with the values-kind.yaml overrides
# (which point at host.docker.internal for the compose infra).
#
# Release names:
#   df-ingest-api, df-stream-processor, df-query-api, df-clickhouse-sink,
#   df-archiver, df-opensky-ingest
#
# Run scripts/kind-up.sh first.
set -euo pipefail

CLUSTER_NAME="dream-flight"
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HELM_DIR="${PROJECT_ROOT}/deploy/helm"

# Go services share the cmd/<svc>/Dockerfile layout; Python services live
# under services/<svc>/ with their own Dockerfile.
# INFRA_CHARTS are charts with no image to build — they use upstream images
# (e.g. postgres-retention runs psql via postgres:16-alpine).
GO_SERVICES=(ingest-api stream-processor query-api clickhouse-sink)
PYTHON_SERVICES=(archiver opensky-ingest)
INFRA_CHARTS=(postgres-retention)

# Charts that DO have a local image to build/load.
BUILT_SERVICES=("${GO_SERVICES[@]}" "${PYTHON_SERVICES[@]}")
# Every chart we helm-install (built or infra).
ALL_CHARTS=("${BUILT_SERVICES[@]}" "${INFRA_CHARTS[@]}")

if ! kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
  echo "Error: kind cluster '${CLUSTER_NAME}' not found. Run scripts/kind-up.sh first."
  exit 1
fi

# ── Build Docker images ────────────────────────────────────────────────────
echo "==> Building Docker images..."
for svc in "${GO_SERVICES[@]}"; do
  image="dream-flight/${svc}:latest"
  echo "  Building ${image}..."
  docker build -q -t "${image}" -f "${PROJECT_ROOT}/cmd/${svc}/Dockerfile" "${PROJECT_ROOT}"
done

# Python services. Build context is the repo root so each Dockerfile can
# COPY shared assets (e.g. internal/avro/flight_telemetry.avsc).
for svc in "${PYTHON_SERVICES[@]}"; do
  image="dream-flight/${svc}:latest"
  echo "  Building ${image}..."
  docker build -q -t "${image}" -f "${PROJECT_ROOT}/services/${svc}/Dockerfile" "${PROJECT_ROOT}"
done

# ── Load images into kind ──────────────────────────────────────────────────
echo "==> Loading images into kind cluster '${CLUSTER_NAME}'..."
for svc in "${BUILT_SERVICES[@]}"; do
  image="dream-flight/${svc}:latest"
  kind load docker-image "${image}" --name "${CLUSTER_NAME}"
done

# ── Install / upgrade Helm charts ──────────────────────────────────────────
echo "==> Installing Helm charts with values-kind.yaml overrides..."
for svc in "${BUILT_SERVICES[@]}"; do
  # tag=latest so `imagePullPolicy: Never` still matches the `kind load`-ed
  # image (which is tagged ":latest", not the Chart.AppVersion).
  helm upgrade --install "df-${svc}" "${HELM_DIR}/${svc}" \
    --values "${HELM_DIR}/${svc}/values-kind.yaml" \
    --set image.tag=latest
done
# Infra charts: no image build, no image.tag override; chart pins its own
# upstream image. values-kind.yaml provides the host.docker.internal DSN.
for chart in "${INFRA_CHARTS[@]}"; do
  helm upgrade --install "df-${chart}" "${HELM_DIR}/${chart}" \
    --values "${HELM_DIR}/${chart}/values-kind.yaml"
done

echo ""
echo "==> All charts applied. Current pod state:"
kubectl get pods -l 'app in (ingest-api,stream-processor,query-api,clickhouse-sink,archiver,opensky-ingest,postgres-retention)'
echo ""
echo "==> NodePorts (from kind, forwarded to host):"
echo "  ingest-api:      http://localhost:8080  (NodePort 30080)"
echo "  argocd-server:   https://localhost:8443 (NodePort 30443, if ArgoCD is installed)"
