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
      # 409 from SR means "schema is incompatible with current latest", NOT
      # "already exists" — idempotent re-registration of the same schema
      # returns 200. Fail loudly so a breaking change cannot sneak through.
      echo "FAILED — incompatible with existing schema (HTTP 409)"
      echo "${body}" >&2
      exit 1
      ;;
    *)
      echo "FAILED (HTTP ${http_code})"
      echo "${body}" >&2
      exit 1
      ;;
  esac
}

pin_compat() {
  local subject="$1"
  local level="$2"
  echo -n "Pinning '${subject}' compatibility to ${level} ... "
  local response http_code body
  response=$(curl -sS -w '\n%{http_code}' \
    -X PUT \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d "{\"compatibility\":\"${level}\"}" \
    "${SR_URL}/config/${subject}")
  http_code="${response##*$'\n'}"
  body="${response%$'\n'*}"
  if [[ "$http_code" == "200" ]]; then
    echo "OK"
  else
    echo "FAILED (HTTP ${http_code})"
    echo "${body}" >&2
    exit 1
  fi
}

echo "Schema Registry: ${SR_URL}"
echo

# -----------------------------------------------------------
# Register each schema under its Kafka-topic-derived subject.
# Convention: <topic>-value for the value schema.
# -----------------------------------------------------------

register "flight.telemetry-value" "${SCHEMAS_DIR}/flight_telemetry.avsc"

# Pin subject-level compat so an operator cannot accidentally relax the
# contract by changing the SR-wide default compatibility level.
pin_compat "flight.telemetry-value" "BACKWARD"

echo
echo "Done. Registered subjects:"
curl -s "${SR_URL}/subjects" | jq .
