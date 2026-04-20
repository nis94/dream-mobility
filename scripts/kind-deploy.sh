#!/usr/bin/env bash
# Builds Docker images for all four Go services, loads them into the
# "dream-mobility" kind cluster, and installs (or upgrades) the Helm charts.
set -euo pipefail

CLUSTER_NAME="dream-mobility"
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HELM_DIR="${PROJECT_ROOT}/deploy/helm"

SERVICES=(ingest-api stream-processor query-api clickhouse-sink)

# ── Ensure the kind cluster exists ──────────────────────────────────────────
if ! kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
  echo "Error: kind cluster '${CLUSTER_NAME}' not found. Run scripts/kind-up.sh first."
  exit 1
fi

# ── Build Docker images ────────────────────────────────────────────────────
echo "==> Building Docker images..."
for svc in "${SERVICES[@]}"; do
  image="dream-mobility/${svc}:latest"
  echo "  Building ${image}..."
  docker build -t "${image}" -f "${PROJECT_ROOT}/cmd/${svc}/Dockerfile" "${PROJECT_ROOT}"
done

# ── Load images into kind ──────────────────────────────────────────────────
echo "==> Loading images into kind cluster '${CLUSTER_NAME}'..."
for svc in "${SERVICES[@]}"; do
  image="dream-mobility/${svc}:latest"
  echo "  Loading ${image}..."
  kind load docker-image "${image}" --name "${CLUSTER_NAME}"
done

# ── Install / upgrade Helm charts ──────────────────────────────────────────
echo "==> Installing Helm charts..."
for svc in "${SERVICES[@]}"; do
  echo "  helm upgrade --install dm-${svc} ${HELM_DIR}/${svc}"
  helm upgrade --install "dm-${svc}" "${HELM_DIR}/${svc}" \
    --set image.pullPolicy=Never
done

echo ""
echo "==> All services deployed. Checking pod status..."
kubectl get pods -l release=dm-ingest-api -l release=dm-stream-processor -l release=dm-query-api -l release=dm-clickhouse-sink 2>/dev/null || true
kubectl get pods
