"""
Kafka → Iceberg/Parquet archiver for the Dream Mobility pipeline.

Consumes movement events from Kafka, buffers them in memory, and flushes
to an Apache Iceberg table backed by MinIO/S3 in Parquet format.

The Iceberg table is partitioned by days(event_ts) and entity_type for
efficient time-range and entity-scoped queries.

Usage:
    uv run python archiver.py
    uv run python archiver.py --bootstrap-servers kafka:9092 --flush-interval 30

Environment variables (override CLI defaults):
    KAFKA_BROKERS           Kafka bootstrap servers (default: localhost:29092)
    KAFKA_TOPIC             Topic to consume (default: movement.events)
    ICEBERG_CATALOG_URI     Iceberg REST catalog URI (default: http://localhost:8181)
    ICEBERG_WAREHOUSE       S3 warehouse path (default: s3://lake/)
    S3_ENDPOINT             MinIO/S3 endpoint (default: http://localhost:9100)
    S3_ACCESS_KEY           MinIO access key (default: minioadmin)
    S3_SECRET_KEY           MinIO secret key (default: minioadmin)
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
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import (
    DoubleType,
    NestedField,
    StringType,
    TimestamptzType,
)

log = logging.getLogger("archiver")

# Arrow schema matching the Avro MovementEvent (flattened).
ARROW_SCHEMA = pa.schema([
    pa.field("event_id", pa.string(), nullable=False),
    pa.field("entity_type", pa.string(), nullable=False),
    pa.field("entity_id", pa.string(), nullable=False),
    pa.field("event_ts", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("lat", pa.float64(), nullable=False),
    pa.field("lon", pa.float64(), nullable=False),
    pa.field("speed_kmh", pa.float64(), nullable=True),
    pa.field("heading_deg", pa.float64(), nullable=True),
    pa.field("accuracy_m", pa.float64(), nullable=True),
    pa.field("source", pa.string(), nullable=True),
    pa.field("attributes", pa.string(), nullable=True),
    pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
])

# Iceberg schema for table creation.
ICEBERG_SCHEMA = Schema(
    NestedField(1, "event_id", StringType(), required=True),
    NestedField(2, "entity_type", StringType(), required=True),
    NestedField(3, "entity_id", StringType(), required=True),
    NestedField(4, "event_ts", TimestamptzType(), required=True),
    NestedField(5, "lat", DoubleType(), required=True),
    NestedField(6, "lon", DoubleType(), required=True),
    NestedField(7, "speed_kmh", DoubleType(), required=False),
    NestedField(8, "heading_deg", DoubleType(), required=False),
    NestedField(9, "accuracy_m", DoubleType(), required=False),
    NestedField(10, "source", StringType(), required=False),
    NestedField(11, "attributes", StringType(), required=False),
    NestedField(12, "ingested_at", TimestamptzType(), required=True),
)


_avro_schema = None


def _get_avro_schema():
    """Lazy-load and cache the parsed Avro schema from the .avsc file."""
    global _avro_schema
    if _avro_schema is None:
        import fastavro

        schema_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "internal", "avro", "movement_event.avsc"
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
    """Map the Avro record (with timestamp-micros as int) to our flat dict."""
    # timestamp is microseconds since epoch.
    ts_micros = rec["timestamp"]
    event_ts = datetime.fromtimestamp(ts_micros / 1_000_000, tz=UTC)

    return {
        "event_id": rec["event_id"],
        "entity_type": rec["entity_type"],
        "entity_id": rec["entity_id"],
        "event_ts": event_ts,
        "lat": rec["lat"],
        "lon": rec["lon"],
        "speed_kmh": rec.get("speed_kmh"),
        "heading_deg": rec.get("heading_deg"),
        "accuracy_m": rec.get("accuracy_m"),
        "source": rec.get("source"),
        "attributes": rec.get("attributes"),
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
        table_name: str = "mobility.raw_events",
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
            partition_spec = PartitionSpec(
                PartitionField(
                    source_id=4, field_id=1000, transform=DayTransform(), name="event_day"
                ),
                PartitionField(
                    source_id=2, field_id=1001, transform=IdentityTransform(), name="entity_type"
                ),
            )
            self.table = self.catalog.create_table(
                self.table_name,
                schema=ICEBERG_SCHEMA,
                partition_spec=partition_spec,
            )
            log.info("created iceberg table: %s", self.table_name)

    def add(self, event: dict[str, Any]) -> None:
        self.buffer.append(event)
        if len(self.buffer) >= self.flush_size:
            self.flush()

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

        table = pa.Table.from_pylist(batch, schema=ARROW_SCHEMA)
        self.table.append(table)
        log.info("flushed %d events to iceberg", len(batch))


def run(args: argparse.Namespace) -> None:
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

    try:
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                if archiver.should_flush():
                    archiver.flush()
                    consumer.commit(asynchronous=False)
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            event = decode_avro_event(msg.value())
            if event is not None:
                had_buffer = len(archiver.buffer) > 0
                archiver.add(event)
                # add() may have triggered a size-based flush internally.
                flushed_by_size = had_buffer and len(archiver.buffer) == 0

                if flushed_by_size or archiver.should_flush():
                    if not flushed_by_size:
                        archiver.flush()
                    consumer.commit(asynchronous=False)
    finally:
        archiver.flush()
        consumer.commit(asynchronous=False)
        consumer.close()
        log.info("archiver stopped")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Kafka → Iceberg archiver")
    p.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BROKERS", "localhost:29092"))
    p.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "movement.events"))
    p.add_argument("--group-id", default="mobility-iceberg")
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
