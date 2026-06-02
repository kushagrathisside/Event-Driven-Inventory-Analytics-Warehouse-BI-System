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


def validate_procurement_silver_ready() -> None:
    validate_silver_ready(get_settings().paths.silver_procurement_path, domain="procurement")


def refresh_procurement_dimensions() -> None:
    # Procurement shares dim_product, dim_supplier, and dim_warehouse with inventory.
    # There is no separate procurement-specific dimension function.
    settings = get_settings()
    execute_statement(settings.analytics_writer_db, "SELECT analytics.refresh_inventory_dimensions();")


def load_procurement_event_facts() -> int:
    settings = get_settings()
    count = call_warehouse_function(
        settings.analytics_writer_db,
        "SELECT analytics.load_procurement_event_facts();",
    )
    logger.info("Procurement facts loaded rows=%s", count)
    return count


def refresh_procurement_semantic_views() -> None:
    settings = get_settings()
    execute_statement(settings.analytics_writer_db, "SELECT analytics.refresh_procurement_semantic_layer();")


def run_procurement_quality_checks() -> list[tuple]:
    settings = get_settings()
    return run_quality_checks(
        settings.analytics_reader_db,
        "SELECT issue_name, issue_count FROM analytics.run_procurement_quality_checks();",
        domain="procurement",
    )


def build_procurement_gold() -> list[tuple]:
    refresh_procurement_dimensions()
    load_procurement_event_facts()
    refresh_procurement_semantic_views()
    return run_procurement_quality_checks()
