"""
OpenSky → Kafka producer for the Dream Flight pipeline.

Polls https://opensky-network.org/api/states/all on a fixed interval, encodes
each aircraft state vector as Avro (FlightTelemetry, with the Confluent SR
wire-format prefix), and produces it to flight.telemetry. Partition key is
icao24, hashed with murmur2 to match the Go ingest-api producer so per-
aircraft ordering holds across both producers if both ever run.

Bypasses ingest-api on purpose — it's a pull-based source with its own poll
cadence; round-tripping through HTTP gains nothing and adds an encode hop.
ingest-api remains the entry point for *push* clients (curl, generator,
external apps).

Rate limit footgun: anonymous OpenSky access = 400 credits/day, 1 credit per
/states/all call. POLL_INTERVAL_SECONDS=300 gives 288 polls/day; do NOT drop
below ~215s without OAuth2 client credentials (4000 credits/day). The fetcher
respects HTTP 429 with exponential backoff so a misconfigured deployment
won't hammer OpenSky.

Usage:
    uv run python opensky_ingest.py
    uv run python opensky_ingest.py --poll-interval 600

Environment variables (override CLI defaults):
    KAFKA_BROKERS                Kafka bootstrap servers (default: localhost:29092)
    KAFKA_TOPIC                  Topic to produce (default: flight.telemetry)
    SCHEMA_REGISTRY_URL          SR base URL (default: http://localhost:8081)
    OPENSKY_URL                  Override the OpenSky endpoint (default below)
    OPENSKY_CLIENT_ID            OAuth2 client id (optional; anonymous if unset)
    OPENSKY_CLIENT_SECRET        OAuth2 client secret (optional)
    POLL_INTERVAL_SECONDS        Seconds between polls (default: 300)
    OTEL_EXPORTER_OTLP_ENDPOINT  OTLP collector URL (default: http://localhost:4318)
    LOG_LEVEL                    DEBUG/INFO/WARN/ERROR (default: INFO)
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
import signal
import struct
import sys
import time
import uuid
from datetime import UTC, datetime
from typing import Any

import fastavro
import requests
from confluent_kafka import KafkaError, KafkaException, Producer
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.propagate import inject
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import SpanKind

log = logging.getLogger("opensky-ingest")

# OpenSky /states/all index map. The endpoint returns each aircraft as a
# 17- or 18-element list; index access is faster than a dict and avoids
# constructing per-row dicts only to throw them away.
IDX_ICAO24 = 0
IDX_CALLSIGN = 1
IDX_ORIGIN_COUNTRY = 2
IDX_TIME_POSITION = 3
IDX_LAST_CONTACT = 4
IDX_LON = 5
IDX_LAT = 6
IDX_BARO_ALTITUDE = 7
IDX_ON_GROUND = 8
IDX_VELOCITY = 9
IDX_TRUE_TRACK = 10
IDX_VERTICAL_RATE = 11
IDX_GEO_ALTITUDE = 13
IDX_SQUAWK = 14
IDX_SPI = 15
IDX_POSITION_SOURCE = 16
IDX_CATEGORY = 17  # Optional 18th column.

# OpenSky integer code → schema enum symbol.
POSITION_SOURCE_MAP = {
    0: "ADSB",
    1: "ASTERIX",
    2: "MLAT",
    3: "FLARM",
}


def _init_tracer(otlp_endpoint: str) -> trace.Tracer:
    """OTel SDK init. Mirrors archiver.py — same collector, same propagator,
    so the shared `flight.telemetry` topic carries traces end-to-end across
    Python (producer + archiver) and Go (sinks)."""
    resource = Resource.create({SERVICE_NAME: "opensky-ingest"})
    provider = TracerProvider(resource=resource)
    traces_url = otlp_endpoint.rstrip("/") + "/v1/traces"
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=traces_url)))
    trace.set_tracer_provider(provider)
    return trace.get_tracer("services.opensky-ingest")


def _fetch_schema(sr_url: str, subject: str) -> tuple[int, dict]:
    """Fetch the latest schema for `subject` from Schema Registry. Returns
    (schema_id, parsed_schema). Cached at startup; we trust the producer
    contract not to change mid-run."""
    url = f"{sr_url.rstrip('/')}/subjects/{subject}/versions/latest"
    resp = requests.get(url, timeout=5)
    resp.raise_for_status()
    data = resp.json()
    schema_id = int(data["id"])
    schema = fastavro.parse_schema(json.loads(data["schema"]))
    log.info("schema cached: subject=%s id=%d", subject, schema_id)
    return schema_id, schema


def _encode_avro(record: dict[str, Any], schema: dict, schema_id: int) -> bytes:
    """Serialize one record as fastavro binary, prepended with the Confluent
    SR wire format: 0x00 magic + 4-byte big-endian schema id."""
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, record)
    return b"\x00" + struct.pack(">I", schema_id) + buf.getvalue()


def _opensky_session(client_id: str | None, client_secret: str | None) -> requests.Session:
    """Returns an HTTP session. With creds, would acquire an OAuth2 token —
    deferred (anonymous works for a learning project; clean room for adding
    later)."""
    sess = requests.Session()
    if client_id and client_secret:
        log.warning(
            "OPENSKY_CLIENT_ID/SECRET set but OAuth2 flow not implemented yet; "
            "falling back to anonymous (400 credits/day)"
        )
    return sess


def _state_to_event(state: list, poll_time: datetime) -> dict[str, Any] | None:
    """Map one OpenSky state vector to a FlightTelemetry record. Returns None
    if the state lacks the required fields (icao24/lat/lon)."""
    icao24 = state[IDX_ICAO24]
    lat = state[IDX_LAT] if len(state) > IDX_LAT else None
    lon = state[IDX_LON] if len(state) > IDX_LON else None
    if not icao24 or lat is None or lon is None:
        return None

    # Prefer time_position (when the position was last updated). Fall back to
    # the poll-envelope time so we always have something monotonic-ish.
    ts = state[IDX_TIME_POSITION] if len(state) > IDX_TIME_POSITION else None
    observed_at = (
        datetime.fromtimestamp(ts, tz=UTC) if ts is not None else poll_time
    )

    # OpenSky uppercases icao24 occasionally; normalize to lowercase to match
    # the schema's contract and the partition-key convention.
    icao24_lc = icao24.strip().lower()
    callsign = state[IDX_CALLSIGN]
    if isinstance(callsign, str):
        callsign = callsign.strip() or None  # OpenSky pads with spaces

    pos_src_int = state[IDX_POSITION_SOURCE] if len(state) > IDX_POSITION_SOURCE else None
    position_source = POSITION_SOURCE_MAP.get(pos_src_int, "UNKNOWN")

    category = state[IDX_CATEGORY] if len(state) > IDX_CATEGORY else None
    squawk = state[IDX_SQUAWK] if len(state) > IDX_SQUAWK else None

    return {
        "event_id": str(uuid.uuid4()),
        "icao24": icao24_lc,
        "callsign": callsign,
        "origin_country": state[IDX_ORIGIN_COUNTRY] or "Unknown",
        "observed_at": observed_at,
        "position_source": position_source,
        "lat": float(lat),
        "lon": float(lon),
        "baro_altitude_m": _safe_float(state, IDX_BARO_ALTITUDE),
        "geo_altitude_m": _safe_float(state, IDX_GEO_ALTITUDE),
        "velocity_ms": _safe_float(state, IDX_VELOCITY),
        "true_track_deg": _safe_float(state, IDX_TRUE_TRACK),
        "vertical_rate_ms": _safe_float(state, IDX_VERTICAL_RATE),
        "on_ground": bool(state[IDX_ON_GROUND]),
        "squawk": squawk,
        "spi": bool(state[IDX_SPI]) if len(state) > IDX_SPI else False,
        "category": int(category) if category is not None else None,
    }


def _safe_float(state: list, idx: int) -> float | None:
    if len(state) <= idx:
        return None
    v = state[idx]
    return float(v) if v is not None else None


def _delivery_callback_factory(metrics: dict[str, int]):
    def _cb(err, _msg):
        if err is not None:
            metrics["delivery_errors"] += 1
            log.warning("delivery failed: %s", err)
        else:
            metrics["delivered"] += 1
    return _cb


def run(args: argparse.Namespace) -> None:
    tracer = _init_tracer(args.otlp_endpoint)

    schema_id, schema = _fetch_schema(args.schema_registry_url, f"{args.topic}-value")

    producer = Producer({
        "bootstrap.servers": args.bootstrap_servers,
        # murmur2_random matches the Go side's segmentio/kafka-go
        # Murmur2Balancer{Consistent:true}. Per-aircraft ordering invariant
        # holds because same icao24 → same partition.
        "partitioner": "murmur2_random",
        "linger.ms": 50,
        "compression.type": "snappy",
    })

    sess = _opensky_session(args.opensky_client_id, args.opensky_client_secret)

    running = True

    def _stop(_sig, _frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    log.info(
        "opensky-ingest starting: brokers=%s topic=%s sr=%s poll=%ds",
        args.bootstrap_servers,
        args.topic,
        args.schema_registry_url,
        args.poll_interval,
    )

    metrics = {"delivered": 0, "delivery_errors": 0, "poll_errors": 0}
    delivery_cb = _delivery_callback_factory(metrics)

    backoff = 1.0
    while running:
        with tracer.start_as_current_span(
            "opensky.poll",
            kind=SpanKind.PRODUCER,
            attributes={"opensky.url": args.opensky_url},
        ) as span:
            poll_start = time.monotonic()
            try:
                resp = sess.get(args.opensky_url, timeout=20)
                if resp.status_code == 429:
                    log.warning("OpenSky rate limit (429); backing off %.1fs", backoff)
                    span.set_attribute("error", True)
                    span.set_attribute("error.kind", "rate_limited")
                    metrics["poll_errors"] += 1
                    _sleep_until(poll_start + backoff, lambda: running)
                    backoff = min(backoff * 2, 600.0)
                    continue
                resp.raise_for_status()
                payload = resp.json()
                backoff = 1.0  # reset on success
            except Exception as e:
                metrics["poll_errors"] += 1
                span.set_attribute("error", True)
                span.set_attribute("error.kind", type(e).__name__)
                log.exception("opensky fetch failed; backing off %.1fs", backoff)
                _sleep_until(poll_start + backoff, lambda: running)
                backoff = min(backoff * 2, 600.0)
                continue

            states = payload.get("states") or []
            poll_time = datetime.fromtimestamp(payload["time"], tz=UTC)
            span.set_attribute("opensky.states_total", len(states))

            produced = 0
            skipped = 0
            with tracer.start_as_current_span("kafka.produce_batch") as batch_span:
                # Inject traceparent ONCE per batch — every message in this
                # poll continues the same trace. ~10k messages × 1 span each
                # would balloon Jaeger; one span per batch is the right
                # granularity for "this poll → these writes".
                headers_dict: dict[str, str] = {}
                inject(headers_dict)
                kafka_headers = [(k, v.encode("utf-8")) for k, v in headers_dict.items()]

                for state in states:
                    event = _state_to_event(state, poll_time)
                    if event is None:
                        skipped += 1
                        continue
                    try:
                        value = _encode_avro(event, schema, schema_id)
                    except Exception:
                        log.exception("avro encode failed for icao24=%s; skipping",
                                      state[IDX_ICAO24])
                        skipped += 1
                        continue
                    try:
                        producer.produce(
                            topic=args.topic,
                            key=event["icao24"].encode("utf-8"),
                            value=value,
                            headers=kafka_headers,
                            on_delivery=delivery_cb,
                        )
                        produced += 1
                    except BufferError:
                        # Local queue full; drain and retry once.
                        producer.poll(0.5)
                        producer.produce(
                            topic=args.topic,
                            key=event["icao24"].encode("utf-8"),
                            value=value,
                            headers=kafka_headers,
                            on_delivery=delivery_cb,
                        )
                        produced += 1
                    except KafkaException as e:
                        log.error("kafka produce error: %s", e)
                        if e.args and isinstance(e.args[0], KafkaError) and e.args[0].fatal():
                            running = False
                            break
                # Flush at end of batch so delivery callbacks settle before
                # the next poll. 10s is a generous bound; under normal
                # operation flush returns far faster.
                producer.flush(timeout=10)

                batch_span.set_attribute("kafka.produced", produced)
                batch_span.set_attribute("kafka.skipped", skipped)

            log.info(
                "poll: states=%d produced=%d skipped=%d delivered_total=%d errors_total=%d",
                len(states), produced, skipped,
                metrics["delivered"], metrics["delivery_errors"],
            )

        _sleep_until(poll_start + args.poll_interval, lambda: running)

    producer.flush(timeout=10)
    log.info("opensky-ingest stopped (delivered=%d errors=%d)",
             metrics["delivered"], metrics["delivery_errors"])


def _sleep_until(deadline: float, alive) -> None:
    """Sleep in 0.5s slices so SIGTERM is honored within half a second
    even when the poll interval is 5 minutes."""
    while alive() and time.monotonic() < deadline:
        time.sleep(min(0.5, deadline - time.monotonic()))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="OpenSky → Kafka producer")
    p.add_argument(
        "--opensky-url",
        default=os.getenv("OPENSKY_URL", "https://opensky-network.org/api/states/all"),
    )
    p.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BROKERS", "localhost:29092"))
    p.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "flight.telemetry"))
    p.add_argument(
        "--schema-registry-url",
        default=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
    )
    p.add_argument(
        "--poll-interval", type=float,
        default=float(os.getenv("POLL_INTERVAL_SECONDS", "300")),
    )
    p.add_argument("--opensky-client-id", default=os.getenv("OPENSKY_CLIENT_ID"))
    p.add_argument("--opensky-client-secret", default=os.getenv("OPENSKY_CLIENT_SECRET"))
    p.add_argument(
        "--otlp-endpoint",
        default=os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318"),
    )
    return p.parse_args(argv)


def main() -> None:
    args = parse_args()
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )
    run(args)


if __name__ == "__main__":
    main()
