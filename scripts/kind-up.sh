#!/usr/bin/env bash
# Creates a kind cluster named "dream-mobility".
# Idempotent: skips creation if the cluster already exists.
set -euo pipefail

CLUSTER_NAME="dream-mobility"

if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
  echo "kind cluster '${CLUSTER_NAME}' already exists — skipping creation."
else
  echo "Creating kind cluster '${CLUSTER_NAME}'..."
  kind create cluster --name "${CLUSTER_NAME}" --wait 60s
  echo "Cluster '${CLUSTER_NAME}' is ready."
fi

kubectl cluster-info --context "kind-${CLUSTER_NAME}"
