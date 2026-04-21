"""
Ad-hoc query CLI for the Dream Mobility Iceberg lake.

Loads the Iceberg table via PyIceberg and runs the caller's SQL through
DuckDB. This is a dev / analyst tool — the query runs in-process against
the caller's own machine, so runaway queries (full-table scans, missing
LIMIT) can OOM the laptop. Safety caps below default to 10k rows and 30s
timeout; both are override-able per invocation.

Usage:
    uv run python query.py "SELECT entity_id, count(*) FROM raw_events GROUP BY 1 ORDER BY 2 DESC LIMIT 10"
    uv run python query.py --limit 100000 --timeout 120 --table mobility.raw_events "SELECT * FROM raw_events"
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
    p.add_argument(
        "--catalog-uri", default=os.getenv("ICEBERG_CATALOG_URI", "http://localhost:8181")
    )
    p.add_argument("--warehouse", default=os.getenv("ICEBERG_WAREHOUSE", "s3://lake/"))
    p.add_argument("--s3-endpoint", default=os.getenv("S3_ENDPOINT", "http://localhost:9100"))
    # S3 credentials must come from env / flag — no hardcoded default so a
    # fat-fingered invocation can't reach a non-local lake with minioadmin.
    p.add_argument("--s3-access-key", default=os.getenv("S3_ACCESS_KEY"))
    p.add_argument("--s3-secret-key", default=os.getenv("S3_SECRET_KEY"))
    p.add_argument(
        "--catalog-token",
        default=os.getenv("ICEBERG_CATALOG_TOKEN"),
        help="Optional bearer token for the Iceberg REST catalog",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=10_000,
        help="Row cap applied to the result (0 = no cap; use with caution)",
    )
    p.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="DuckDB statement timeout in seconds (0 = no timeout)",
    )
    args = p.parse_args()

    if not args.s3_access_key or not args.s3_secret_key:
        print(
            "error: S3 credentials required. Set S3_ACCESS_KEY / S3_SECRET_KEY "
            "or pass --s3-access-key / --s3-secret-key.",
            file=sys.stderr,
        )
        sys.exit(2)

    catalog_kwargs = {
        "uri": args.catalog_uri,
        "s3.endpoint": args.s3_endpoint,
        "s3.access-key-id": args.s3_access_key,
        "s3.secret-access-key": args.s3_secret_key,
        "warehouse": args.warehouse,
    }
    if args.catalog_token:
        catalog_kwargs["token"] = args.catalog_token
    catalog = load_catalog("rest", **catalog_kwargs)

    table = catalog.load_table(args.table)
    arrow_table = table.scan().to_arrow()

    short_name = args.table.split(".")[-1]
    con = duckdb.connect()
    con.register(short_name, arrow_table)

    if args.timeout > 0:
        # statement_timeout expects milliseconds as a string literal.
        con.execute(f"SET statement_timeout = '{args.timeout * 1000}ms'")

    sql = args.sql
    if args.limit > 0 and "limit" not in sql.lower():
        sql = f"SELECT * FROM ({sql}) _wrapped LIMIT {args.limit}"

    result = con.execute(sql).fetchdf()
    print(result.to_string(index=False))


if __name__ == "__main__":
    main()
