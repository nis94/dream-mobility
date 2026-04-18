"""
Synthetic movement event generator for the Dream Mobility ingestion pipeline.

Emits JSON events shaped like:

    {
      "event_id": "<uuid>",
      "entity": {"type": "vehicle", "id": "vehicle-7"},
      "timestamp": "2025-01-01T10:15:00Z",
      "position": {"lat": 52.5200, "lon": 13.4050},
      "speed_kmh": 42.3,
      "source": "gps",
      "attributes": {"battery_level": 0.82}
    }

Models:
- A pool of N entities, each performing a random walk around a starting point.
- Optional duplicate injection: with probability p_dup, an emitted event is
  re-sent shortly after with the same event_id (tests dedupe).
- Optional out-of-order injection: with probability p_ooo, an emitted event's
  timestamp is shifted backwards by up to `out_of_order_window_s` seconds,
  simulating a late-arriving event (tests timestamp-gated upsert).

Output targets:
- "stdout"                         (default) -- one JSON object per line
- "file:./events.jsonl"            -- write to a JSONL file
- "http://host:port/events"        -- POST one-by-one (Phase 2+)
- "http://host:port/events:batch"  -- POST as a batch (Phase 2+)

Run:
    uv run python gen.py --rate 100 --duration 30 --duplicates 0.05 --out-of-order 0.10
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import random
import sys
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

log = logging.getLogger("generator")

# A handful of plausible base coordinates (Berlin, NYC, Tokyo, TLV, SF).
BASE_COORDS: list[tuple[float, float]] = [
    (52.5200, 13.4050),
    (40.7128, -74.0060),
    (35.6762, 139.6503),
    (32.0853, 34.7818),
    (37.7749, -122.4194),
]

ENTITY_TYPES: list[str] = ["vehicle", "courier", "scooter", "device"]


@dataclass
class Entity:
    """One entity performing a random walk in lat/lon space."""

    type: str
    id: str
    lat: float
    lon: float
    heading_deg: float
    speed_kmh: float

    def step(self, dt_s: float) -> None:
        # Random walk: small heading & speed jitter, then advance position.
        self.heading_deg = (self.heading_deg + random.uniform(-15.0, 15.0)) % 360.0
        self.speed_kmh = max(0.0, min(120.0, self.speed_kmh + random.uniform(-3.0, 3.0)))
        # ~111 km per degree of latitude; longitude scaled by cos(lat). Good enough for fake data.
        distance_km = self.speed_kmh * (dt_s / 3600.0)
        d_lat = distance_km / 111.0
        d_lon = distance_km / (111.0 * max(0.1, abs(_cos_deg(self.lat))))
        # Project along heading (0=N, 90=E)
        rad = _deg_to_rad(self.heading_deg)
        self.lat += d_lat * _cos(rad)
        self.lon += d_lon * _sin(rad)


def _deg_to_rad(d: float) -> float:
    return d * math.pi / 180.0


def _cos(x: float) -> float:
    return math.cos(x)


def _sin(x: float) -> float:
    return math.sin(x)


def _cos_deg(d: float) -> float:
    return _cos(_deg_to_rad(d))


def make_entities(count: int, rng: random.Random) -> list[Entity]:
    entities: list[Entity] = []
    for i in range(count):
        etype = rng.choice(ENTITY_TYPES)
        base_lat, base_lon = rng.choice(BASE_COORDS)
        entities.append(
            Entity(
                type=etype,
                id=f"{etype}-{i}",
                lat=base_lat + rng.uniform(-0.05, 0.05),
                lon=base_lon + rng.uniform(-0.05, 0.05),
                heading_deg=rng.uniform(0.0, 360.0),
                speed_kmh=rng.uniform(0.0, 60.0),
            )
        )
    return entities


def event_from(entity: Entity, when: datetime, rng: random.Random) -> dict[str, Any]:
    """Build one event dict from an entity's current state."""
    payload: dict[str, Any] = {
        "event_id": str(uuid.uuid4()),
        "entity": {"type": entity.type, "id": entity.id},
        "timestamp": when.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "position": {"lat": round(entity.lat, 6), "lon": round(entity.lon, 6)},
    }
    # Optional fields, sometimes omitted (mirrors the "optional or missing fields" spec).
    if rng.random() < 0.9:
        payload["speed_kmh"] = round(entity.speed_kmh, 2)
    if rng.random() < 0.7:
        payload["heading_deg"] = round(entity.heading_deg, 2)
    if rng.random() < 0.5:
        payload["accuracy_m"] = round(rng.uniform(1.0, 25.0), 2)
    if rng.random() < 0.8:
        payload["source"] = rng.choice(["gps", "wifi", "cell"])
    if rng.random() < 0.4:
        payload["attributes"] = {"battery_level": round(rng.uniform(0.05, 1.0), 2)}
    return payload


# --- Sinks --------------------------------------------------------------------


class Sink:
    async def emit(self, events: list[dict[str, Any]]) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        pass


class StdoutSink(Sink):
    async def emit(self, events: list[dict[str, Any]]) -> None:
        for e in events:
            sys.stdout.write(json.dumps(e, separators=(",", ":")) + "\n")
        sys.stdout.flush()


class FileSink(Sink):
    def __init__(self, path: str) -> None:
        # Open once, write many, close once: streaming sink pattern, not a
        # leaked file handle (see close() below). ruff's SIM115 expects a
        # context manager which doesn't fit the open-across-async-emits shape.
        self._fh = open(path, "a", buffering=1)  # noqa: SIM115

    async def emit(self, events: list[dict[str, Any]]) -> None:
        for e in events:
            self._fh.write(json.dumps(e, separators=(",", ":")) + "\n")

    async def close(self) -> None:
        self._fh.close()


class HttpSink(Sink):
    def __init__(self, url: str, batch: bool) -> None:
        self._url = url
        self._batch = batch
        self._client = httpx.AsyncClient(timeout=10.0)

    async def emit(self, events: list[dict[str, Any]]) -> None:
        if self._batch:
            r = await self._client.post(self._url, json={"events": events})
            r.raise_for_status()
        else:
            await asyncio.gather(*(self._post_one(e) for e in events))

    async def _post_one(self, e: dict[str, Any]) -> None:
        r = await self._client.post(self._url, json=e)
        r.raise_for_status()

    async def close(self) -> None:
        await self._client.aclose()


def make_sink(target: str) -> Sink:
    if target == "stdout":
        return StdoutSink()
    if target.startswith("file:"):
        return FileSink(target[len("file:") :])
    if target.startswith("http://") or target.startswith("https://"):
        if target.endswith(":batch"):
            return HttpSink(target[: -len(":batch")], batch=True)
        return HttpSink(target, batch=False)
    raise ValueError(f"Unsupported target: {target}")


# --- Generator loop ----------------------------------------------------------


async def run(args: argparse.Namespace) -> None:
    rng = random.Random(args.seed)
    entities = make_entities(args.entities, rng)
    sink = make_sink(args.target)

    end_at = datetime.now(UTC) + timedelta(seconds=args.duration) if args.duration > 0 else None
    events_emitted = 0
    duplicates_emitted = 0
    out_of_order_emitted = 0

    # Buffer of recently-emitted events for duplicate replay.
    recent: list[dict[str, Any]] = []
    recent_max = 1000

    tick_interval = 1.0 / max(1.0, args.rate / max(1, args.batch_size))
    log.info(
        "starting: entities=%d rate=%d/s batch=%d target=%s duration=%s",
        args.entities,
        args.rate,
        args.batch_size,
        args.target,
        f"{args.duration}s" if args.duration > 0 else "infinite",
    )
    try:
        while True:
            if end_at is not None and datetime.now(UTC) >= end_at:
                break

            # Step the simulation by ~1s per emission tick (independent of wall clock cadence).
            for ent in entities:
                ent.step(dt_s=1.0)

            now = datetime.now(UTC)
            batch: list[dict[str, Any]] = []
            for _ in range(args.batch_size):
                # Either emit a duplicate of a recently-seen event (keeping
                # batch size honest: one roll → one slot), or build a fresh
                # event. On the first few iterations `recent` is empty, so
                # the duplicate branch never fires.
                if recent and rng.random() < args.duplicates:
                    batch.append(rng.choice(recent))
                    duplicates_emitted += 1
                    continue

                ent = rng.choice(entities)
                ts = now
                if rng.random() < args.out_of_order:
                    shift = rng.uniform(1.0, args.out_of_order_window_s)
                    ts = now - timedelta(seconds=shift)
                    out_of_order_emitted += 1
                ev = event_from(ent, ts, rng)
                batch.append(ev)

                recent.append(ev)
                if len(recent) > recent_max:
                    recent.pop(0)

            await sink.emit(batch)
            events_emitted += len(batch)

            await asyncio.sleep(tick_interval)
    finally:
        await sink.close()
        log.info(
            "done: emitted=%d duplicates=%d out_of_order=%d",
            events_emitted,
            duplicates_emitted,
            out_of_order_emitted,
        )


# --- CLI ---------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Dream Mobility synthetic event generator")
    p.add_argument("--rate", type=int, default=50, help="Events per second (target)")
    p.add_argument("--batch-size", type=int, default=10, help="Events emitted per tick")
    p.add_argument("--entities", type=int, default=20, help="Number of distinct entities")
    p.add_argument(
        "--duration",
        type=int,
        default=0,
        help="Run for N seconds (0 = run until interrupted)",
    )
    p.add_argument(
        "--duplicates",
        type=float,
        default=0.0,
        help="Probability per emitted event of also re-sending a previously-seen event (0..1)",
    )
    p.add_argument(
        "--out-of-order",
        type=float,
        default=0.0,
        help="Probability per emitted event of having its timestamp shifted backwards (0..1)",
    )
    p.add_argument(
        "--out-of-order-window-s",
        type=float,
        default=60.0,
        help="Max backwards shift in seconds for out-of-order events",
    )
    p.add_argument(
        "--target",
        default="stdout",
        help="Where to send events: stdout | file:./path | http://host:port/events[:batch]",
    )
    p.add_argument("--seed", type=int, default=42, help="RNG seed for reproducibility")
    p.add_argument("--log-level", default="INFO", help="DEBUG | INFO | WARNING | ERROR")
    return p.parse_args(argv)


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
