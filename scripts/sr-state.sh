#!/usr/bin/env bash
# Dump the current Schema Registry state: subjects, global config, and per-
# subject latest schema with field-level default coverage. Useful for
# verifying schema-evolution assumptions before / after a deploy.
#
# Optional: --probe-evolution adds a synthetic nullable-with-default field to
# the latest MovementEvent schema and runs the SR compatibility check against
# the current latest. Reports whether forward evolution is unblocked.

set -euo pipefail

SR_URL="${SR_URL:-http://localhost:8081}"
PROBE=""
for arg in "$@"; do
  case "${arg}" in
    --probe-evolution) PROBE="1" ;;
    -h|--help)
      echo "Usage: $0 [--probe-evolution]"
      exit 0
      ;;
    *)
      echo "unknown arg: ${arg}" >&2; exit 2
      ;;
  esac
done

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required but not installed (brew install jq)" >&2
  exit 1
fi

if ! curl -fsS --max-time 2 "${SR_URL}/subjects" >/dev/null 2>&1; then
  echo "Schema Registry unreachable at ${SR_URL} — run 'make up' first" >&2
  exit 2
fi

echo "=== Schema Registry: ${SR_URL} ==="
echo
echo "-- global config --"
curl -s "${SR_URL}/config" | jq .

echo
echo "-- subjects --"
curl -s "${SR_URL}/subjects" | jq .

for subject in $(curl -s "${SR_URL}/subjects" | jq -r '.[]'); do
  echo
  echo "-- ${subject} (latest) --"
  curl -s "${SR_URL}/subjects/${subject}/versions/latest" | jq '{
    id,
    version,
    fields: [
      .schema | fromjson | .fields[] | {
        name,
        type,
        has_default: (has("default"))
      }
    ]
  }'
done

if [[ -n "${PROBE}" ]]; then
  echo
  echo "=== Evolution probe ==="
  subject="movement.events-value"
  if ! curl -fsS --max-time 2 "${SR_URL}/subjects/${subject}/versions/latest" >/dev/null 2>&1; then
    echo "subject ${subject} not found; skipping probe" >&2
    exit 0
  fi

  # Fetch latest schema, parse its fields, append a new nullable-with-default
  # field, and ask SR if the result is compatible with the latest version.
  probe_schema=$(curl -s "${SR_URL}/subjects/${subject}/versions/latest" | jq -r '
    .schema | fromjson | .fields += [{
      name: "probe_trip_id",
      type: ["null", "string"],
      default: null,
      doc: "sr-state.sh evolution probe — never merged"
    }] | @json
  ')
  payload=$(jq -n --arg schema "${probe_schema}" '{schema: $schema}')

  result=$(curl -s -X POST \
    -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
    -d "${payload}" \
    "${SR_URL}/compatibility/subjects/${subject}/versions/latest")

  compat=$(echo "${result}" | jq -r '.is_compatible // "unknown"')
  echo "adding nullable-with-default field to ${subject} → is_compatible=${compat}"
  if [[ "${compat}" != "true" ]]; then
    echo "raw response: ${result}" >&2
    exit 1
  fi
fi
