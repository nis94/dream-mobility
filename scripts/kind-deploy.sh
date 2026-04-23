#!/usr/bin/env bash
# Builds Docker images for all four Go services, loads them into the
# "dream-mobility" kind cluster, and installs (or upgrades) the Helm charts
# with the values-kind.yaml overrides (which point at host.docker.internal
# for the compose infra).
#
# The four services each have a short release name:
#   dm-ingest-api, dm-stream-processor, dm-query-api, dm-clickhouse-sink
#
# Run scripts/kind-up.sh first.
set -euo pipefail

CLUSTER_NAME="dream-mobility"
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HELM_DIR="${PROJECT_ROOT}/deploy/helm"

SERVICES=(ingest-api stream-processor query-api clickhouse-sink)

if ! kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
  echo "Error: kind cluster '${CLUSTER_NAME}' not found. Run scripts/kind-up.sh first."
  exit 1
fi

# ── Build Docker images ────────────────────────────────────────────────────
echo "==> Building Docker images..."
for svc in "${SERVICES[@]}"; do
  image="dream-mobility/${svc}:latest"
  echo "  Building ${image}..."
  docker build -q -t "${image}" -f "${PROJECT_ROOT}/cmd/${svc}/Dockerfile" "${PROJECT_ROOT}"
done

# ── Load images into kind ──────────────────────────────────────────────────
echo "==> Loading images into kind cluster '${CLUSTER_NAME}'..."
for svc in "${SERVICES[@]}"; do
  image="dream-mobility/${svc}:latest"
  kind load docker-image "${image}" --name "${CLUSTER_NAME}"
done

# ── Install / upgrade Helm charts ──────────────────────────────────────────
echo "==> Installing Helm charts with values-kind.yaml overrides..."
for svc in "${SERVICES[@]}"; do
  # tag=latest so `imagePullPolicy: Never` still matches the `kind load`-ed
  # image (which is tagged ":latest", not the Chart.AppVersion).
  helm upgrade --install "dm-${svc}" "${HELM_DIR}/${svc}" \
    --values "${HELM_DIR}/${svc}/values-kind.yaml" \
    --set image.tag=latest
done

echo ""
echo "==> All charts applied. Current pod state:"
# `app in (...)` is a set-based selector; the previous version tried
# `release in (...)` with a list of release names, which excludes all pods
# because no single pod carries every release label.
kubectl get pods -l 'app in (ingest-api,stream-processor,query-api,clickhouse-sink)'
echo ""
echo "==> NodePorts (from kind, forwarded to host):"
echo "  ingest-api:      http://localhost:8080  (NodePort 30080)"
echo "  argocd-server:   https://localhost:8443 (NodePort 30443, if ArgoCD is installed)"
