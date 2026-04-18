#!/usr/bin/env bash
# Register Avro schemas with the Confluent Schema Registry.
# Idempotent: re-running updates the schema if compatible, or no-ops if unchanged.
#
# Usage:
#   ./scripts/register-schemas.sh                          # default: http://localhost:8081
#   SCHEMA_REGISTRY_URL=http://sr:8081 ./scripts/register-schemas.sh

set -euo pipefail

SR_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCHEMAS_DIR="${SCRIPT_DIR}/../internal/avro"

register() {
  local subject="$1"
  local schema_file="$2"

  # Schema Registry expects the Avro schema JSON-encoded inside a wrapper object.
  # jq -Rs reads the file as a raw string; we embed it in {"schema": "..."}.
  local payload
  payload=$(jq -n --arg schema "$(cat "$schema_file")" '{"schemaType": "AVRO", "schema": $schema}')

  echo -n "Registering subject '${subject}' ... "
  local http_code
  http_code=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d "$payload" \
    "${SR_URL}/subjects/${subject}/versions")

  if [[ "$http_code" == "200" ]]; then
    echo "OK (registered/updated)"
  elif [[ "$http_code" == "409" ]]; then
    echo "OK (already exists, compatible)"
  else
    echo "FAILED (HTTP ${http_code})"
    # Print the full response for debugging
    curl -s \
      -X POST \
      -H "Content-Type: application/vnd.schemaregistry.v1+json" \
      -d "$payload" \
      "${SR_URL}/subjects/${subject}/versions"
    echo
    exit 1
  fi
}

echo "Schema Registry: ${SR_URL}"
echo

# -----------------------------------------------------------
# Register each schema under its Kafka-topic-derived subject.
# Convention: <topic>-value for the value schema.
# -----------------------------------------------------------

register "movement.events-value" "${SCHEMAS_DIR}/movement_event.avsc"

echo
echo "Done. Registered subjects:"
curl -s "${SR_URL}/subjects" | jq .
