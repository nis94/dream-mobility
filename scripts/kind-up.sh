#!/usr/bin/env bash
# Creates a kind cluster named "dream-mobility" with host port mappings
# that expose:
#   - ingest-api NodePort 30080 → host 8080  (generator / curl)
#   - argocd-server NodePort 30443 → host 8443  (GitOps UI)
# Idempotent: skips creation if the cluster already exists.
set -euo pipefail

CLUSTER_NAME="dream-mobility"
CONFIG_FILE="$(cd "$(dirname "$0")/.." && pwd)/deploy/kind/kind-cluster.yaml"

if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
  echo "kind cluster '${CLUSTER_NAME}' already exists — skipping creation."
else
  echo "Creating kind cluster '${CLUSTER_NAME}' from ${CONFIG_FILE}..."
  kind create cluster --name "${CLUSTER_NAME}" --config "${CONFIG_FILE}" --wait 60s
  echo "Cluster '${CLUSTER_NAME}' is ready."
fi

kubectl cluster-info --context "kind-${CLUSTER_NAME}"
