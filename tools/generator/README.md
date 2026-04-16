# Synthetic Movement Event Generator

A small async Python tool that emits realistic GPS-like movement events for the
Dream Mobility ingestion pipeline. Used as a test data source in every phase
from 0 onwards.

## Install

```bash
# uv handles venv + install + lockfile in one go
uv sync
```

If you don't have `uv`, install it with `brew install uv` (mac) or
`curl -LsSf https://astral.sh/uv/install.sh | sh`.

## Run

```bash
# Phase 0: dump 50 events/sec to stdout for 10 seconds
uv run python gen.py --rate 50 --duration 10

# Mix in 5% duplicates and 10% out-of-order to exercise dedupe logic
uv run python gen.py --rate 100 --duration 30 --duplicates 0.05 --out-of-order 0.10

# Write to a JSONL file
uv run python gen.py --rate 100 --duration 30 --target file:./events.jsonl

# Phase 2+: send to the ingestion API (single-event POST)
uv run python gen.py --rate 100 --target http://localhost:8080/events

# Phase 2+: send as batches
uv run python gen.py --rate 1000 --batch-size 50 --target http://localhost:8080/events:batch
```

## CLI

| Flag | Default | What it does |
|------|---------|--------------|
| `--rate N` | 50 | Target events/sec |
| `--batch-size N` | 10 | Events per tick |
| `--entities N` | 20 | Pool of distinct entities (each does a random walk) |
| `--duration S` | 0 (forever) | Run for S seconds then exit |
| `--duplicates P` | 0.0 | Per-event chance of re-sending a previously emitted event with same `event_id` |
| `--out-of-order P` | 0.0 | Per-event chance of shifting timestamp backwards |
| `--out-of-order-window-s S` | 60 | Max backwards shift in seconds |
| `--target` | `stdout` | `stdout` \| `file:./path` \| `http://host:port/events[:batch]` |
| `--seed N` | 42 | RNG seed (reproducibility) |
| `--log-level` | `INFO` | Logging verbosity |

## Event shape

Matches the example in `task.pdf`:

```json
{
  "event_id": "uuid",
  "entity": {"type": "vehicle", "id": "vehicle-7"},
  "timestamp": "2025-01-01T10:15:00.123456Z",
  "position": {"lat": 52.5200, "lon": 13.4050},
  "speed_kmh": 42.3,
  "heading_deg": 137.5,
  "accuracy_m": 4.2,
  "source": "gps",
  "attributes": {"battery_level": 0.82}
}
```

Optional fields are sometimes omitted (per the task spec: "may contain optional or missing fields").
