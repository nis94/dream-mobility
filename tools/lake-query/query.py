"""
Ad-hoc query CLI for the Dream Mobility Iceberg lake.

Uses PyIceberg to load the table and DuckDB to run SQL queries over the
Parquet files. This demonstrates the lake's query surface without needing
a full Trino/Spark deployment.

Usage:
    uv run python query.py "SELECT entity_id, count(*) FROM raw_events GROUP BY 1 ORDER BY 2 DESC LIMIT 10"
    uv run python query.py --table mobility.raw_events "SELECT * FROM raw_events LIMIT 5"
"""

from __future__ import annotations

import argparse
import os
import sys

import duckdb
from pyiceberg.catalog import load_catalog


def main() -> None:
    p = argparse.ArgumentParser(description="Query the Iceberg lake via DuckDB")
    p.add_argument("sql", help="SQL query to run (table name = short name, e.g. 'raw_events')")
    p.add_argument("--table", default="mobility.raw_events", help="Iceberg table identifier")
    p.add_argument("--catalog-uri", default=os.getenv("ICEBERG_CATALOG_URI", "http://localhost:8181"))
    p.add_argument("--warehouse", default=os.getenv("ICEBERG_WAREHOUSE", "s3://lake/"))
    p.add_argument("--s3-endpoint", default=os.getenv("S3_ENDPOINT", "http://localhost:9100"))
    p.add_argument("--s3-access-key", default=os.getenv("S3_ACCESS_KEY", "minioadmin"))
    p.add_argument("--s3-secret-key", default=os.getenv("S3_SECRET_KEY", "minioadmin"))
    args = p.parse_args()

    catalog = load_catalog(
        "rest",
        **{
            "uri": args.catalog_uri,
            "s3.endpoint": args.s3_endpoint,
            "s3.access-key-id": args.s3_access_key,
            "s3.secret-access-key": args.s3_secret_key,
            "warehouse": args.warehouse,
        },
    )

    table = catalog.load_table(args.table)
    arrow_table = table.scan().to_arrow()

    # Register the Arrow table as a DuckDB view so SQL queries work naturally.
    short_name = args.table.split(".")[-1]
    con = duckdb.connect()
    con.register(short_name, arrow_table)

    result = con.execute(args.sql).fetchdf()
    print(result.to_string(index=False))


if __name__ == "__main__":
    main()
