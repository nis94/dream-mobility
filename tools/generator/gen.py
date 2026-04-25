"""
Synthetic flight telemetry generator for the Dream Flight pipeline.

Now repurposed as a load / chaos tool — opensky-ingest is the primary data
source. This script remains the only deterministic way to test:
  * --duplicates       (replay a recent event with the same event_id)
  * --out-of-order     (shift observed_at backwards to test the timestamp
                        upsert gate)

Emits JSON events shaped like:

    {
      "event_id": "<uuid>",
      "icao24": "abc123",
      "callsign": "BAW123",
      "origin_country": "GB",
      "observed_at": "2025-01-01T10:15:00Z",
      "position_source": "ADSB",
      "lat": 52.52, "lon": 13.40,
      "baro_altitude_m": 11000.0,
      "velocity_ms": 245.5,
      "true_track_deg": 90.0,
      "vertical_rate_ms": 0.0,
      "on_ground": false,
      "spi": false
    }

Models a pool of N synthetic aircraft. Each one has a fixed icao24 hex ID, a
callsign with the airline prefix, an origin country, and a random walk in
lat/lon. Altitude/velocity/heading sampled per emission.

Output targets:
- "stdout"                         (default) -- one JSON object per line
- "file:./events.jsonl"            -- JSONL file
- "http://host:port/events"        -- POST one-by-one to ingest-api
- "http://host:port/events:batch"  -- POST as a batch

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

# Base coordinates over major air-traffic regions (UK, Germany, France, US East,
# US West). Each synthetic aircraft starts within ±2° of one of these.
BASE_COORDS: list[tuple[float, float]] = [
    (51.5, -0.5),     # London
    (50.0, 8.5),      # Frankfurt
    (48.85, 2.35),    # Paris
    (40.7, -74.0),    # NYC
    (37.6, -122.4),   # SF
]

# (airline ICAO prefix, country) — 3-char prefix is the canonical operator
# code; downstream rollups slice by it.
AIRLINES: list[tuple[str, str]] = [
    ("BAW", "United Kingdom"),
    ("DLH", "Germany"),
    ("AFR", "France"),
    ("UAL", "United States"),
    ("AAL", "United States"),
    ("KLM", "Netherlands"),
    ("IBE", "Spain"),
    ("SWR", "Switzerland"),
    ("AUA", "Austria"),
    ("SAS", "Sweden"),
]

POSITION_SOURCES: list[str] = ["ADSB", "MLAT", "ASTERIX", "FLARM"]


@dataclass
class Aircraft:
    """One synthetic aircraft performing a random walk at cruise altitude."""

    icao24: str
    callsign: str
    origin_country: str
    lat: float
    lon: float
    true_track_deg: float
    velocity_ms: float
    altitude_m: float
    vertical_rate_ms: float
    on_ground: bool

    def step(self, dt_s: float) -> None:
        # Random walk: small heading & speed jitter, then advance position.
        # Aviation cruise speeds: 200-280 m/s (720-1000 km/h). Light planes
        # 50-80 m/s. Range covers both.
        self.true_track_deg = (self.true_track_deg + random.uniform(-5.0, 5.0)) % 360.0
        self.velocity_ms = max(40.0, min(280.0, self.velocity_ms + random.uniform(-5.0, 5.0)))
        # Altitude drift: small most of the time, occasional climb/descent.
        if random.random() < 0.05:
            self.vertical_rate_ms = random.uniform(-15.0, 15.0)
        else:
            self.vertical_rate_ms *= 0.7  # decay
        self.altitude_m = max(0.0, min(13000.0, self.altitude_m + self.vertical_rate_ms * dt_s))
        # Position update. Aircraft cover ~1 m / m/s / s ground distance;
        # converting to lat/lon at the equator: 1° lat ≈ 111 km.
        distance_km = self.velocity_ms * dt_s / 1000.0
        d_lat = distance_km / 111.0
        d_lon = distance_km / (111.0 * max(0.1, abs(_cos_deg(self.lat))))
        rad = _deg_to_rad(self.true_track_deg)
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


def make_aircraft(count: int, rng: random.Random) -> list[Aircraft]:
    """Build N synthetic aircraft. Each gets a stable icao24 hex (used as
    Kafka partition key downstream, so per-aircraft ordering is preserved)."""
    aircraft: list[Aircraft] = []
    for i in range(count):
        # 6-char lowercase hex; deterministic per index given the seed.
        icao24 = f"{(0xa00000 + i):06x}"
        prefix, country = rng.choice(AIRLINES)
        callsign = f"{prefix}{rng.randint(100, 9999)}"
        base_lat, base_lon = rng.choice(BASE_COORDS)
        on_ground = rng.random() < 0.05  # ~5% on the ground
        aircraft.append(
            Aircraft(
                icao24=icao24,
                callsign=callsign,
                origin_country=country,
                lat=base_lat + rng.uniform(-2.0, 2.0),
                lon=base_lon + rng.uniform(-2.0, 2.0),
                true_track_deg=rng.uniform(0.0, 360.0),
                velocity_ms=rng.uniform(80.0, 260.0),
                altitude_m=0.0 if on_ground else rng.uniform(3000.0, 12000.0),
                vertical_rate_ms=0.0,
                on_ground=on_ground,
            )
        )
    return aircraft


def event_from(ac: Aircraft, when: datetime, rng: random.Random) -> dict[str, Any]:
    """Build one event dict from an aircraft's current state."""
    payload: dict[str, Any] = {
        "event_id": str(uuid.uuid4()),
        "icao24": ac.icao24,
        "callsign": ac.callsign,
        "origin_country": ac.origin_country,
        "observed_at": when.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "position_source": rng.choice(POSITION_SOURCES),
        "lat": round(ac.lat, 6),
        "lon": round(ac.lon, 6),
        "on_ground": ac.on_ground,
        "spi": False,
    }
    # Most fields are reported by ADS-B but a few drop out periodically.
    if not ac.on_ground:
        if rng.random() < 0.95:
            payload["baro_altitude_m"] = round(ac.altitude_m, 1)
        if rng.random() < 0.85:
            payload["geo_altitude_m"] = round(ac.altitude_m + rng.uniform(-50.0, 50.0), 1)
        if rng.random() < 0.95:
            payload["velocity_ms"] = round(ac.velocity_ms, 2)
        if rng.random() < 0.95:
            payload["true_track_deg"] = round(ac.true_track_deg, 2)
        if rng.random() < 0.90:
            payload["vertical_rate_ms"] = round(ac.vertical_rate_ms, 2)
    if rng.random() < 0.30:
        # 4 octal digits (0-7). Realistic distribution skews toward
        # everyday ATC codes (1200, 2000-7000); we don't need that fidelity.
        payload["squawk"] = "".join(str(rng.randint(0, 7)) for _ in range(4))
    if rng.random() < 0.20:
        payload["category"] = rng.randint(0, 11)
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
    aircraft = make_aircraft(args.entities, rng)
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
        "starting: aircraft=%d rate=%d/s batch=%d target=%s duration=%s",
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

            # Step the simulation by ~1s per tick (independent of wall clock).
            for ac in aircraft:
                ac.step(dt_s=1.0)

            now = datetime.now(UTC)
            batch: list[dict[str, Any]] = []
            for _ in range(args.batch_size):
                # Replay a recent event (same event_id) with probability
                # --duplicates. The first few ticks have an empty buffer so
                # the duplicate branch never fires until we've emitted
                # something fresh.
                if recent and rng.random() < args.duplicates:
                    batch.append(rng.choice(recent))
                    duplicates_emitted += 1
                    continue

                ac = rng.choice(aircraft)
                ts = now
                if rng.random() < args.out_of_order:
                    shift = rng.uniform(1.0, args.out_of_order_window_s)
                    ts = now - timedelta(seconds=shift)
                    out_of_order_emitted += 1
                ev = event_from(ac, ts, rng)
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
    p = argparse.ArgumentParser(description="Dream Flight synthetic telemetry generator")
    p.add_argument("--rate", type=int, default=50, help="Events per second (target)")
    p.add_argument("--batch-size", type=int, default=10, help="Events emitted per tick")
    p.add_argument("--entities", type=int, default=20, help="Number of distinct aircraft")
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
