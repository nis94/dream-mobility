"""
Kafka → Iceberg/Parquet archiver for the Dream Flight pipeline.

Consumes flight telemetry from Kafka, buffers it in memory, and flushes
to an Apache Iceberg table backed by MinIO/S3 in Parquet format.

The Iceberg table is partitioned by days(observed_at) and origin_country —
both are highly selective ("UK flights last week" prunes ~95% of files).

Usage:
    uv run python archiver.py
    uv run python archiver.py --bootstrap-servers kafka:9092 --flush-interval 30

Environment variables (override CLI defaults):
    KAFKA_BROKERS           Kafka bootstrap servers (default: localhost:29092)
    KAFKA_TOPIC             Topic to consume (default: flight.telemetry)
    ICEBERG_CATALOG_URI     Iceberg REST catalog URI (default: http://localhost:8181)
    ICEBERG_WAREHOUSE       S3 warehouse path (default: s3://lake/)
    S3_ENDPOINT             MinIO/S3 endpoint (default: http://localhost:9100)
    S3_ACCESS_KEY           MinIO access key (required, no default)
    S3_SECRET_KEY           MinIO secret key (required, no default)
    OTEL_EXPORTER_OTLP_ENDPOINT  OTLP collector URL (default: http://localhost:4318)
"""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import os
import signal
import sys
import time
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa
from confluent_kafka import Consumer, KafkaError, KafkaException
from opentelemetry import propagate, trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import SpanKind
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import NullOrder, SortDirection, SortField, SortOrder
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    NestedField,
    StringType,
    TimestamptzType,
)

log = logging.getLogger("archiver")


def _init_tracer(otlp_endpoint: str) -> trace.Tracer:
    """Initialize the OTel SDK so Kafka-carried W3C TraceContext from the Go
    producer continues into Python spans exported to the same collector.

    Called once at startup; safe to call multiple times (SDK replaces the
    global provider).
    """
    resource = Resource.create({SERVICE_NAME: "archiver"})
    provider = TracerProvider(resource=resource)
    # `endpoint` expects the full path ending in /v1/traces; the OTel spec
    # constant OTEL_EXPORTER_OTLP_ENDPOINT usually does NOT include that
    # suffix, so append it here.
    traces_url = otlp_endpoint.rstrip("/") + "/v1/traces"
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=traces_url)))
    trace.set_tracer_provider(provider)
    return trace.get_tracer("services.archiver")


def _extract_parent_context(headers: list[tuple[str, bytes]] | None):
    """Turn Kafka headers into a W3C TraceContext parent. Returns the
    extracted Context, which start_as_current_span uses as the parent.
    Missing / malformed headers yield a root context (new trace).
    """
    if not headers:
        return None
    carrier = {k: v.decode("utf-8", errors="replace") for k, v in headers}
    return propagate.extract(carrier)

# Arrow schema matching the Avro FlightTelemetry record. Field order MUST
# match ICEBERG_SCHEMA below — Arrow → Iceberg conversion is positional.
ARROW_SCHEMA = pa.schema([
    pa.field("event_id", pa.string(), nullable=False),
    pa.field("icao24", pa.string(), nullable=False),
    pa.field("callsign", pa.string(), nullable=True),
    pa.field("origin_country", pa.string(), nullable=False),
    pa.field("observed_at", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("position_source", pa.string(), nullable=False),
    pa.field("lat", pa.float64(), nullable=False),
    pa.field("lon", pa.float64(), nullable=False),
    pa.field("baro_altitude_m", pa.float64(), nullable=True),
    pa.field("geo_altitude_m", pa.float64(), nullable=True),
    pa.field("velocity_ms", pa.float64(), nullable=True),
    pa.field("true_track_deg", pa.float64(), nullable=True),
    pa.field("vertical_rate_ms", pa.float64(), nullable=True),
    pa.field("on_ground", pa.bool_(), nullable=False),
    pa.field("squawk", pa.string(), nullable=True),
    pa.field("spi", pa.bool_(), nullable=False),
    pa.field("category", pa.int32(), nullable=True),
    pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
])

# Iceberg schema. Field IDs renumbered cleanly 1..N matching Avro field order
# so a fresh table create is unambiguous; never reuse an old ID across schema
# evolutions because Iceberg metadata keys files by field ID, not by name.
ICEBERG_SCHEMA = Schema(
    NestedField(1, "event_id", StringType(), required=True),
    NestedField(2, "icao24", StringType(), required=True),
    NestedField(3, "callsign", StringType(), required=False),
    NestedField(4, "origin_country", StringType(), required=True),
    NestedField(5, "observed_at", TimestamptzType(), required=True),
    NestedField(6, "position_source", StringType(), required=True),
    NestedField(7, "lat", DoubleType(), required=True),
    NestedField(8, "lon", DoubleType(), required=True),
    NestedField(9, "baro_altitude_m", DoubleType(), required=False),
    NestedField(10, "geo_altitude_m", DoubleType(), required=False),
    NestedField(11, "velocity_ms", DoubleType(), required=False),
    NestedField(12, "true_track_deg", DoubleType(), required=False),
    NestedField(13, "vertical_rate_ms", DoubleType(), required=False),
    NestedField(14, "on_ground", BooleanType(), required=True),
    NestedField(15, "squawk", StringType(), required=False),
    NestedField(16, "spi", BooleanType(), required=True),
    NestedField(17, "category", IntegerType(), required=False),
    NestedField(18, "ingested_at", TimestamptzType(), required=True),
)


_avro_schema = None


def _get_avro_schema():
    """Lazy-load and cache the parsed Avro schema from the .avsc file."""
    global _avro_schema
    if _avro_schema is None:
        import fastavro

        schema_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "internal", "avro", "flight_telemetry.avsc"
        )
        with open(schema_path) as f:
            _avro_schema = fastavro.parse_schema(json.load(f))
    return _avro_schema


def decode_avro_event(raw: bytes) -> dict[str, Any] | None:
    """Strip SR wire format (5-byte prefix) and decode Avro via fastavro.

    Returns a flat dict matching ARROW_SCHEMA, or None if decoding fails.
    """
    if len(raw) < 6 or raw[0] != 0:
        log.warning("invalid wire format (len=%d)", len(raw))
        return None

    try:
        import io as _io

        import fastavro

        rec = fastavro.schemaless_reader(_io.BytesIO(raw[5:]), _get_avro_schema())
        return _avro_record_to_dict(rec)
    except Exception:
        log.exception("avro decode failed")
        return None


def _avro_record_to_dict(rec: dict[str, Any]) -> dict[str, Any]:
    """Map the Avro FlightTelemetry record to a flat dict matching ARROW_SCHEMA.
    fastavro decodes `timestamp-micros` as a timezone-aware `datetime`, so we
    take it as-is.
    """
    ts = rec["observed_at"]
    observed_at = (
        ts if isinstance(ts, datetime) else datetime.fromtimestamp(ts / 1_000_000, tz=UTC)
    )

    # fastavro returns uuid.UUID for logicalType=uuid; Arrow expects string.
    # str() is a no-op on strings so we skip the isinstance branch.
    return {
        "event_id": str(rec["event_id"]),
        "icao24": rec["icao24"],
        "callsign": rec.get("callsign"),
        "origin_country": rec["origin_country"],
        "observed_at": observed_at,
        "position_source": rec["position_source"],
        "lat": rec["lat"],
        "lon": rec["lon"],
        "baro_altitude_m": rec.get("baro_altitude_m"),
        "geo_altitude_m": rec.get("geo_altitude_m"),
        "velocity_ms": rec.get("velocity_ms"),
        "true_track_deg": rec.get("true_track_deg"),
        "vertical_rate_ms": rec.get("vertical_rate_ms"),
        "on_ground": rec["on_ground"],
        "squawk": rec.get("squawk"),
        "spi": rec["spi"],
        "category": rec.get("category"),
        "ingested_at": datetime.now(UTC),
    }


class IcebergArchiver:
    """Buffers events and flushes to an Iceberg table as Parquet."""

    def __init__(
        self,
        catalog_uri: str,
        warehouse: str,
        s3_endpoint: str,
        s3_access_key: str,
        s3_secret_key: str,
        table_name: str = "flight.telemetry",
        flush_size: int = 10000,
        flush_interval: float = 30.0,
        catalog_token: str | None = None,
    ):
        self.flush_size = flush_size
        self.flush_interval = flush_interval
        self.table_name = table_name
        self.buffer: list[dict[str, Any]] = []
        self.last_flush = time.monotonic()

        # Configure the Iceberg catalog. The REST catalog supports an
        # optional bearer token — without it the catalog is effectively
        # unauthenticated, which is fine for the dev image but unsafe for
        # any non-local deployment.
        catalog_kwargs: dict[str, Any] = {
            "uri": catalog_uri,
            "s3.endpoint": s3_endpoint,
            "s3.access-key-id": s3_access_key,
            "s3.secret-access-key": s3_secret_key,
            "warehouse": warehouse,
        }
        if catalog_token:
            catalog_kwargs["token"] = catalog_token
        self.catalog = load_catalog("rest", **catalog_kwargs)

        self._ensure_table()

    def _ensure_table(self) -> None:
        namespace, _name = self.table_name.split(".", 1)
        with contextlib.suppress(NamespaceAlreadyExistsError):
            self.catalog.create_namespace(namespace)

        try:
            self.table = self.catalog.load_table(self.table_name)
            log.info("loaded existing iceberg table: %s", self.table_name)
        except NoSuchTableError:
            # Partition by day(observed_at) + identity(origin_country). Both
            # are highly selective for typical queries — "UK flights last
            # week" prunes ~95% of files. source_id values are the field IDs
            # in ICEBERG_SCHEMA: observed_at=5, origin_country=4.
            partition_spec = PartitionSpec(
                PartitionField(
                    source_id=5, field_id=1000, transform=DayTransform(), name="observed_day"
                ),
                PartitionField(
                    source_id=4, field_id=1001, transform=IdentityTransform(), name="origin_country"
                ),
            )
            # Sort by observed_at within each partition so Parquet row groups
            # are time-clustered and reader-side min/max pruning on time-range
            # queries stays tight as the table grows.
            sort_order = SortOrder(
                SortField(
                    source_id=5,
                    transform=IdentityTransform(),
                    direction=SortDirection.ASC,
                    null_order=NullOrder.NULLS_LAST,
                )
            )
            self.table = self.catalog.create_table(
                self.table_name,
                schema=ICEBERG_SCHEMA,
                partition_spec=partition_spec,
                sort_order=sort_order,
            )
            log.info("created iceberg table: %s", self.table_name)

    def add(self, event: dict[str, Any]) -> bool:
        """Append an event. Returns True if the buffer triggered a flush."""
        self.buffer.append(event)
        if len(self.buffer) >= self.flush_size:
            self.flush()
            return True
        return False

    def should_flush(self) -> bool:
        if not self.buffer:
            return False
        return (time.monotonic() - self.last_flush) >= self.flush_interval

    def flush(self) -> None:
        if not self.buffer:
            return

        batch = self.buffer
        self.buffer = []
        self.last_flush = time.monotonic()

        # In-batch dedup by event_id. Postgres and ClickHouse absorb
        # broker-retry duplicates at the sink (PK / ReplacingMergeTree);
        # Iceberg has no native uniqueness, so we dedup within the flush
        # window here. This catches the common case (broker resends the
        # same message seconds later). Cross-flush duplicates still
        # require read-time dedup — see tools/lake-query/query.py.
        seen: set[str] = set()
        deduped: list[dict[str, Any]] = []
        for event in batch:
            event_id = event["event_id"]
            if event_id in seen:
                continue
            seen.add(event_id)
            deduped.append(event)
        dropped = len(batch) - len(deduped)
        if dropped > 0:
            log.info("in-batch dedup dropped %d duplicate event_ids", dropped)

        table = pa.Table.from_pylist(deduped, schema=ARROW_SCHEMA)
        self.table.append(table)
        log.info("flushed %d events to iceberg (pre-dedup %d)", len(deduped), len(batch))


def run(args: argparse.Namespace) -> None:
    tracer = _init_tracer(args.otlp_endpoint)

    archiver = IcebergArchiver(
        catalog_uri=args.catalog_uri,
        warehouse=args.warehouse,
        s3_endpoint=args.s3_endpoint,
        s3_access_key=args.s3_access_key,
        s3_secret_key=args.s3_secret_key,
        catalog_token=args.catalog_token,
        flush_size=args.flush_size,
        flush_interval=args.flush_interval,
    )

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap_servers,
        "group.id": args.group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([args.topic])

    running = True

    def _stop(sig, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    log.info(
        "archiver starting: brokers=%s topic=%s group=%s catalog=%s",
        args.bootstrap_servers,
        args.topic,
        args.group_id,
        args.catalog_uri,
    )

    # backoff is only grown/reset in response to error vs successful-message
    # events, NOT idle polls — resetting on every None would defeat the cap
    # under a noisy-but-non-fatal broker state that alternates idle / error.
    backoff = 1.0
    try:
        while running:
            try:
                msg = consumer.poll(timeout=1.0)
            except KafkaException as e:
                log.error("kafka poll failed, backing off: %s", e)
                _deadline = time.monotonic() + backoff
                while running and time.monotonic() < _deadline:
                    time.sleep(min(0.5, _deadline - time.monotonic()))
                backoff = min(backoff * 2, 30.0)
                continue

            if msg is None:
                if archiver.should_flush():
                    archiver.flush()
                    _safe_commit(consumer)
                continue

            if msg.error():
                err = msg.error()
                if err.code() == KafkaError._PARTITION_EOF:
                    continue
                if err.fatal():
                    log.error("fatal kafka error, exiting: %s", err)
                    running = False
                    break
                # Transient — exponential backoff so a persistent-but-
                # recoverable error doesn't produce one log line per second
                # indefinitely.
                log.warning("kafka message error (transient): %s", err)
                _deadline = time.monotonic() + min(backoff, 30.0)
                while running and time.monotonic() < _deadline:
                    time.sleep(min(0.5, _deadline - time.monotonic()))
                backoff = min(backoff * 2, 30.0)
                continue

            # Continue the Go-side trace. The producer injects W3C
            # TraceContext into the Kafka message's headers; we extract it
            # here so the consumer span shows up as a child of the original
            # ingest.event span in Jaeger.
            parent_ctx = _extract_parent_context(msg.headers())
            with tracer.start_as_current_span(
                "archiver.consume",
                context=parent_ctx,
                kind=SpanKind.CONSUMER,
                attributes={
                    "messaging.system": "kafka",
                    "messaging.destination.name": msg.topic(),
                    "messaging.kafka.partition": msg.partition(),
                    "messaging.kafka.offset": msg.offset(),
                },
            ) as span:
                event = decode_avro_event(msg.value())
                if event is not None:
                    span.set_attribute("event.id", event["event_id"])
                    span.set_attribute("entity.type", event["entity_type"])
                    span.set_attribute("entity.id", event["entity_id"])
                    # add() returns True when it triggered a size-based flush.
                    flushed = archiver.add(event)
                    if not flushed and archiver.should_flush():
                        archiver.flush()
                        flushed = True
                    if flushed:
                        _safe_commit(consumer)
                else:
                    span.set_attribute("error", True)
                    span.set_attribute("error.kind", "avro_decode_failed")
            # Successful message path — reset the error backoff.
            backoff = 1.0
    finally:
        final_flush_ok = True
        try:
            archiver.flush()
        except Exception:
            final_flush_ok = False
            log.exception(
                "final flush failed; NOT committing offsets — events will redeliver on restart"
            )
        if final_flush_ok:
            _safe_commit(consumer)
        consumer.close()
        log.info("archiver stopped")


def _safe_commit(consumer: Consumer) -> None:
    """Commit offsets, tolerating:
      - `_NO_OFFSET` (first commit with nothing processed yet / post-rebalance)
      - non-`KafkaError` wrappers some older cimpl versions produce
      - any unexpected exception — downgrade to WARNING so a commit hiccup
        does not crash the whole archiver.
    """
    try:
        consumer.commit(asynchronous=False)
    except KafkaException as e:
        code = None
        if e.args and isinstance(e.args[0], KafkaError):
            code = e.args[0].code()
        if code == KafkaError._NO_OFFSET:
            log.debug("no offset to commit yet")
            return
        log.warning("commit failed (non-fatal): %s", e)
    except Exception:
        log.exception("unexpected error during commit")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Kafka → Iceberg archiver")
    p.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BROKERS", "localhost:29092"))
    p.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "flight.telemetry"))
    p.add_argument("--group-id", default="flight-iceberg")
    p.add_argument("--catalog-uri", default=os.getenv("ICEBERG_CATALOG_URI", "http://localhost:8181"))
    p.add_argument("--warehouse", default=os.getenv("ICEBERG_WAREHOUSE", "s3://lake/"))
    p.add_argument("--s3-endpoint", default=os.getenv("S3_ENDPOINT", "http://localhost:9100"))
    # S3 credentials must be supplied via env or flag — no hardcoded default
    # so a real deployment cannot accidentally ship with minioadmin/minioadmin.
    p.add_argument("--s3-access-key", default=os.getenv("S3_ACCESS_KEY"))
    p.add_argument("--s3-secret-key", default=os.getenv("S3_SECRET_KEY"))
    p.add_argument(
        "--catalog-token",
        default=os.getenv("ICEBERG_CATALOG_TOKEN"),
        help="Optional bearer token for the Iceberg REST catalog",
    )
    p.add_argument("--flush-size", type=int, default=10000)
    p.add_argument("--flush-interval", type=float, default=30.0)
    p.add_argument(
        "--otlp-endpoint",
        default=os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318"),
        help="Base OTLP/HTTP endpoint; /v1/traces is appended automatically",
    )
    return p.parse_args(argv)


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )
    if not args.s3_access_key or not args.s3_secret_key:
        log.error(
            "S3 credentials are required; set S3_ACCESS_KEY / S3_SECRET_KEY "
            "or pass --s3-access-key / --s3-secret-key (use minioadmin for local dev)"
        )
        sys.exit(2)
    run(args)


if __name__ == "__main__":
    main()
