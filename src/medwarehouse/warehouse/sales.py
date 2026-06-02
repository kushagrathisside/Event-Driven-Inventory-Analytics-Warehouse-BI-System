from __future__ import annotations

from medwarehouse.config import get_settings
from medwarehouse.logging import get_logger
from medwarehouse.warehouse._common import (
    call_warehouse_function,
    run_quality_checks,
    validate_silver_ready,
)
from medwarehouse.warehouse.sql_runner import execute_statement


logger = get_logger(__name__)


def validate_sales_silver_ready() -> None:
    validate_silver_ready(get_settings().paths.silver_sales_path, domain="sales")


def refresh_sales_dimensions() -> None:
    # Sales uses the shared product/warehouse dimensions plus the sales-specific customer dim.
    settings = get_settings()
    execute_statement(settings.analytics_writer_db, "SELECT analytics.refresh_inventory_dimensions();")
    execute_statement(settings.analytics_writer_db, "SELECT analytics.refresh_dim_customer();")


def load_sales_event_facts() -> int:
    settings = get_settings()
    count = call_warehouse_function(
        settings.analytics_writer_db,
        "SELECT analytics.load_sales_event_facts();",
    )
    logger.info("Sales facts loaded rows=%s", count)
    return count


def refresh_sales_semantic_views() -> None:
    settings = get_settings()
    execute_statement(settings.analytics_writer_db, "SELECT analytics.refresh_sales_semantic_layer();")


def run_sales_quality_checks() -> list[tuple]:
    settings = get_settings()
    return run_quality_checks(
        settings.analytics_reader_db,
        "SELECT issue_name, issue_count FROM analytics.run_sales_quality_checks();",
        domain="sales",
    )


def build_sales_gold() -> list[tuple]:
    refresh_sales_dimensions()
    load_sales_event_facts()
    refresh_sales_semantic_views()
    return run_sales_quality_checks()
