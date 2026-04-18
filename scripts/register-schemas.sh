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

  if [[ ! -r "$schema_file" ]]; then
    echo "schema file not readable: $schema_file" >&2
    exit 2
  fi

  # Schema Registry expects the Avro schema JSON-encoded inside a wrapper object.
  # jq -n builds a new object; --arg binds $schema to the file contents
  # as a string, escaping shell quoting hazards.
  local payload
  payload=$(jq -n --arg schema "$(cat "$schema_file")" \
    '{"schemaType": "AVRO", "schema": $schema}')

  echo -n "Registering subject '${subject}' ... "

  # One POST, capture both body and HTTP code: append the code on a new line
  # via curl -w, then split.
  local response http_code body
  response=$(curl -sS -w '\n%{http_code}' \
    -X POST \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d "$payload" \
    "${SR_URL}/subjects/${subject}/versions")
  http_code="${response##*$'\n'}"
  body="${response%$'\n'*}"

  case "$http_code" in
    200)
      echo "OK (registered/updated)"
      ;;
    409)
      echo "OK (already exists, compatible)"
      ;;
    *)
      echo "FAILED (HTTP ${http_code})"
      echo "${body}" >&2
      exit 1
      ;;
  esac
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
