"""Shared warehouse utilities used across all domain modules."""
from __future__ import annotations

from pathlib import Path

from medwarehouse.logging import get_logger
from medwarehouse.warehouse.sql_runner import execute_statement, fetch_rows
from medwarehouse.config import PostgresConnection


logger = get_logger(__name__)


def validate_silver_ready(silver_path: Path, *, domain: str) -> None:
    """Raise FileNotFoundError if the Silver Parquet dataset for a domain is absent."""
    if not silver_path.exists():
        raise FileNotFoundError(f"{domain} Silver dataset does not exist: {silver_path}")
    parquet_files = list(silver_path.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"{domain} Silver dataset has no parquet files: {silver_path}")
    logger.info("%s Silver ready path=%s files=%s", domain, silver_path, len(parquet_files))


def call_warehouse_function(connection: PostgresConnection, sql: str) -> int:
    """
    Call a scalar warehouse function and return its integer result.
    Used for load/refresh procedures that return affected row counts.
    """
    rows = fetch_rows(connection, sql)
    return int(rows[0][0]) if rows else 0


def run_quality_checks(connection: PostgresConnection, sql: str, *, domain: str) -> list[tuple]:
    """Run a quality-check table function and raise on any failing assertions."""
    rows = fetch_rows(connection, sql)
    failing = [row for row in rows if row[1] and int(row[1]) > 0]
    if failing:
        raise RuntimeError(f"{domain} quality checks failed: {failing}")
    logger.info("%s quality checks passed %s", domain, rows)
    return rows
