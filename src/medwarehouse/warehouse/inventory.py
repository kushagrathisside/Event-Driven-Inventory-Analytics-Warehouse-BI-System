from __future__ import annotations

from pathlib import Path

from medwarehouse.config import AppSettings, get_settings
from medwarehouse.logging import get_logger
from medwarehouse.warehouse.sql_runner import execute_sql_file, execute_statement, fetch_rows


logger = get_logger(__name__)


def _sql_context(settings: AppSettings) -> dict[str, str]:
    return {
        "MASTER_DB_HOST": settings.master_db.host,
        "MASTER_DB_PORT": str(settings.master_db.port),
        "MASTER_DB_NAME": settings.master_db.database,
        "FDW_REMOTE_USER": settings.warehouse_roles.fdw_remote_user,
        "FDW_REMOTE_PASSWORD": settings.warehouse_roles.fdw_remote_password,
        "FDW_READER_USER": settings.warehouse_roles.fdw_reader_user,
        "FDW_READER_PASSWORD": settings.warehouse_roles.fdw_reader_password,
        "SPARK_WRITER_USER": settings.warehouse_roles.spark_writer_user,
        "SPARK_WRITER_PASSWORD": settings.warehouse_roles.spark_writer_password,
        "ANALYTICS_READER_USER": settings.warehouse_roles.analytics_reader_user,
        "ANALYTICS_READER_PASSWORD": settings.warehouse_roles.analytics_reader_password,
    }


def bootstrap_warehouse(sql_root: Path | None = None) -> None:
    settings = get_settings()
    sql_root = settings.paths.sql_root if sql_root is None else sql_root
    context = _sql_context(settings)
    for sql_path in sorted(sql_root.glob("*.sql")):
        execute_sql_file(settings.analytics_admin_db, sql_path, context)


def validate_inventory_silver_ready() -> None:
    settings = get_settings()
    silver_root = settings.paths.silver_inventory_path
    if not silver_root.exists():
        raise FileNotFoundError(f"Silver dataset does not exist: {silver_root}")
    parquet_files = list(silver_root.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"Silver dataset has no parquet files: {silver_root}")
    logger.info("Silver dataset is available path=%s files=%s", silver_root, len(parquet_files))


def refresh_inventory_dimensions() -> None:
    settings = get_settings()
    execute_statement(
        settings.analytics_writer_db,
        "SELECT analytics.refresh_inventory_dimensions();",
    )


def load_inventory_event_facts() -> None:
    settings = get_settings()
    execute_statement(
        settings.analytics_writer_db,
        "SELECT analytics.load_inventory_event_facts();",
    )


def refresh_inventory_semantic_views() -> None:
    settings = get_settings()
    execute_statement(
        settings.analytics_writer_db,
        "SELECT analytics.refresh_inventory_semantic_layer();",
    )


def run_inventory_quality_checks() -> list[tuple]:
    settings = get_settings()
    rows = fetch_rows(
        settings.analytics_writer_db,
        "SELECT issue_name, issue_count FROM analytics.run_inventory_quality_checks();",
    )
    failing = [row for row in rows if row[1] and int(row[1]) > 0]
    if failing:
        raise RuntimeError(f"Inventory quality checks failed: {failing}")
    logger.info("Inventory quality checks passed %s", rows)
    return rows


def build_inventory_gold() -> list[tuple]:
    refresh_inventory_dimensions()
    load_inventory_event_facts()
    refresh_inventory_semantic_views()
    return run_inventory_quality_checks()
