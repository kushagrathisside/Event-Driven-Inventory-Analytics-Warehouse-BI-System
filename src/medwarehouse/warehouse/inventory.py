from __future__ import annotations

from pathlib import Path

from medwarehouse.config import AppSettings, get_settings
from medwarehouse.logging import get_logger
from medwarehouse.warehouse._common import (
    call_warehouse_function,
    run_quality_checks,
    validate_silver_ready,
)
from medwarehouse.warehouse.sql_runner import execute_sql_file, execute_statement


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
        # Read directly from connection objects — single source of truth
        "SPARK_WRITER_USER": settings.analytics_writer_db.user,
        "SPARK_WRITER_PASSWORD": settings.analytics_writer_db.password,
        "ANALYTICS_READER_USER": settings.analytics_reader_db.user,
        "ANALYTICS_READER_PASSWORD": settings.analytics_reader_db.password,
    }


def bootstrap_warehouse(sql_root: Path | None = None) -> None:
    settings = get_settings()
    sql_root = settings.paths.sql_root if sql_root is None else sql_root
    context = _sql_context(settings)
    for sql_path in sorted(sql_root.glob("*.sql")):
        execute_sql_file(settings.analytics_admin_db, sql_path, context)


def validate_inventory_silver_ready() -> None:
    validate_silver_ready(get_settings().paths.silver_inventory_path, domain="inventory")


def refresh_inventory_dimensions() -> None:
    settings = get_settings()
    execute_statement(settings.analytics_writer_db, "SELECT analytics.refresh_inventory_dimensions();")


def load_inventory_event_facts() -> None:
    settings = get_settings()
    execute_statement(settings.analytics_writer_db, "SELECT analytics.load_inventory_event_facts();")


def refresh_inventory_semantic_views() -> None:
    settings = get_settings()
    execute_statement(settings.analytics_writer_db, "SELECT analytics.refresh_inventory_semantic_layer();")


def run_inventory_quality_checks() -> list[tuple]:
    settings = get_settings()
    return run_quality_checks(
        settings.analytics_reader_db,
        "SELECT issue_name, issue_count FROM analytics.run_inventory_quality_checks();",
        domain="inventory",
    )


def build_inventory_gold() -> list[tuple]:
    refresh_inventory_dimensions()
    load_inventory_event_facts()
    refresh_inventory_semantic_views()
    return run_inventory_quality_checks()


def refresh_inventory_balance() -> int:
    settings = get_settings()
    count = call_warehouse_function(settings.analytics_writer_db, "SELECT analytics.refresh_inventory_balance();")
    logger.info("Inventory balance snapshot refreshed rows=%s", count)
    return count


def check_reorder_thresholds() -> int:
    settings = get_settings()
    count = call_warehouse_function(settings.analytics_writer_db, "SELECT analytics.check_reorder_thresholds();")
    logger.info("Reorder threshold check complete draft_orders_created=%s", count)
    return count
