#!/usr/bin/env bash
# End-to-end smoke test for the ingestion API.
#
# - Requires the compose stack to be up (make up).
# - Starts ingest-api if port 8080 is free; otherwise uses the existing one.
#   Any API this script starts is cleaned up on exit.
# - Runs curl against /health and POST /events covering: happy single,
#   batch with mixed valid/invalid, empty batch, bad Content-Type,
#   oversized body, validation failure.
# - Reports pass/fail summary. Exits 0 on all-pass, 1 on any failure.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
API_URL="${API_URL:-http://localhost:8080}"
SR_URL="${SR_URL:-http://localhost:8081}"

PASS=0
FAIL=0

pass() { printf "  PASS  %s\n" "$1"; PASS=$((PASS+1)); }
fail() { printf "  FAIL  %s — %s\n" "$1" "$2"; FAIL=$((FAIL+1)); }

# ---- Preflight --------------------------------------------------------------

if ! curl -fsS --max-time 2 "${SR_URL}/subjects" >/dev/null 2>&1; then
  echo "Schema Registry unreachable at ${SR_URL} — run 'make up' first" >&2
  exit 2
fi

OWN_API_PID=""
cleanup() {
  if [[ -n "${OWN_API_PID}" ]]; then
    kill "${OWN_API_PID}" 2>/dev/null || true
    wait "${OWN_API_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

if ! curl -fsS --max-time 1 "${API_URL}/health" >/dev/null 2>&1; then
  echo "starting ingest-api ..."
  (cd "${REPO_ROOT}" && go run ./cmd/ingest-api) >/tmp/ingest-api.smoke.log 2>&1 &
  OWN_API_PID=$!
  for _ in $(seq 1 30); do
    if curl -fsS --max-time 1 "${API_URL}/health" >/dev/null 2>&1; then
      break
    fi
    sleep 0.5
  done
  if ! curl -fsS --max-time 1 "${API_URL}/health" >/dev/null 2>&1; then
    echo "ingest-api failed to come up; log tail:" >&2
    tail -30 /tmp/ingest-api.smoke.log >&2
    exit 1
  fi
fi

# ---- Helpers ----------------------------------------------------------------

status_only() {
  # Usage: status_only <curl args...>
  # Prints the HTTP status code and discards body.
  curl -s -o /dev/null -w '%{http_code}' "$@" || echo "000"
}

check() {
  local name="$1" expected="$2" got="$3"
  if [[ "$got" == "$expected" ]]; then
    pass "${name}"
  else
    fail "${name}" "expected ${expected}, got ${got}"
  fi
}

# ---- Tests ------------------------------------------------------------------

echo "smoke test: ${API_URL}"
echo

got=$(status_only "${API_URL}/health")
check "GET /health → 200" 200 "${got}"

got=$(status_only -X POST "${API_URL}/events" \
  -H 'Content-Type: application/json' \
  --data-binary "@${REPO_ROOT}/testdata/sample_event.json")
check "POST single event → 202" 202 "${got}"

got=$(status_only -X POST "${API_URL}/events" \
  -H 'Content-Type: application/json' \
  --data-binary "@${REPO_ROOT}/testdata/sample_batch.json")
check "POST batch (2 valid, 1 invalid) → 202" 202 "${got}"

got=$(status_only -X POST "${API_URL}/events" \
  -H 'Content-Type: application/json' \
  --data-binary '{"events":[]}')
check "POST empty batch wrapper → 202" 202 "${got}"

got=$(status_only -X POST "${API_URL}/events" \
  -H 'Content-Type: text/plain' \
  --data-binary 'nope')
check "POST bad Content-Type → 415" 415 "${got}"

# 11 MiB of zero bytes: dd closes its output cleanly so pipefail is happy
# (a `yes | head -c ...` pipeline triggers SIGPIPE on yes and fails the whole
# script under set -euo pipefail).
got=$(dd if=/dev/zero bs=1048576 count=11 2>/dev/null | status_only -X POST "${API_URL}/events" \
  -H 'Content-Type: application/json' \
  --data-binary @-)
check "POST 11 MiB body → 413" 413 "${got}"

got=$(status_only -X POST "${API_URL}/events" \
  -H 'Content-Type: application/json' \
  --data-binary '{"event_id":"","entity":{"type":"v","id":"1"},"timestamp":"2025-01-01T10:00:00Z","position":{"lat":0,"lon":0}}')
check "POST all-invalid event → 400" 400 "${got}"

# ---- Summary ----------------------------------------------------------------

echo
echo "smoke: ${PASS} passed, ${FAIL} failed"
[[ "${FAIL}" -eq 0 ]]
