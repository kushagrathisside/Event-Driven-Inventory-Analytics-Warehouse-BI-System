from __future__ import annotations

import argparse

from medwarehouse.config import get_settings
from medwarehouse.logging import configure_logging
from medwarehouse.platform.app import run_platform_server


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="medwarehouse")
    parser.add_argument("--log-level", default="INFO")
    subparsers = parser.add_subparsers(dest="command", required=True)

    produce = subparsers.add_parser("produce")
    produce_sub = produce.add_subparsers(dest="producer_name", required=True)
    for producer_name in ("inventory", "procurement", "sales"):
        producer_parser = produce_sub.add_parser(producer_name)
        producer_parser.add_argument("--max-events", type=int, default=None)
        producer_parser.add_argument("--dry-run", action="store_true")

    spark = subparsers.add_parser("spark")
    spark_sub = spark.add_subparsers(dest="spark_job", required=True)
    for bronze_cmd in ("inventory-bronze", "procurement-bronze", "sales-bronze"):
        b = spark_sub.add_parser(bronze_cmd)
        b.add_argument("--starting-offsets", default="earliest")
    for silver_cmd in ("inventory-silver", "procurement-silver", "sales-silver",
                       "stage-inventory-events", "stage-procurement-events", "stage-sales-events"):
        spark_sub.add_parser(silver_cmd)

    warehouse = subparsers.add_parser("warehouse")
    warehouse_sub = warehouse.add_subparsers(dest="warehouse_command", required=True)
    for wh_cmd in (
        "bootstrap",
        "refresh-dimensions",
        "load-facts",
        "refresh-views",
        "quality-checks",
        "inventory-gold",
        "refresh-balance",
        "check-reorders",
        # Procurement
        "load-procurement-facts",
        "refresh-procurement-views",
        "procurement-quality-checks",
        "procurement-gold",
        # Sales
        "refresh-sales-dimensions",
        "load-sales-facts",
        "refresh-sales-views",
        "sales-quality-checks",
        "sales-gold",
    ):
        warehouse_sub.add_parser(wh_cmd)

    orchestration = subparsers.add_parser("orchestration")
    orchestration_sub = orchestration.add_subparsers(
        dest="orchestration_command", required=True
    )
    for orch_cmd in (
        "validate-silver",
        "stage-events",
        "build-gold",
        "validate-procurement-silver",
        "stage-procurement-events",
        "build-procurement-gold",
        "validate-sales-silver",
        "stage-sales-events",
        "build-sales-gold",
    ):
        orchestration_sub.add_parser(orch_cmd)

    platform = subparsers.add_parser("platform")
    platform_sub = platform.add_subparsers(dest="platform_command", required=True)
    serve = platform_sub.add_parser("serve")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8787)
    serve.add_argument("--debug", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    configure_logging(args.log_level)
    get_settings()

    if args.command == "produce":
        from medwarehouse.producers.inventory import run_inventory_producer
        from medwarehouse.producers.procurement import run_procurement_producer
        from medwarehouse.producers.sales import run_sales_producer

        if args.producer_name == "inventory":
            run_inventory_producer(max_events=args.max_events, dry_run=args.dry_run)
        elif args.producer_name == "procurement":
            run_procurement_producer(max_events=args.max_events, dry_run=args.dry_run)
        elif args.producer_name == "sales":
            run_sales_producer(max_events=args.max_events, dry_run=args.dry_run)
        return 0

    if args.command == "spark":
        from medwarehouse.spark.jobs.inventory_bronze import run_inventory_bronze
        from medwarehouse.spark.jobs.inventory_silver import run_inventory_silver
        from medwarehouse.spark.jobs.inventory_stage import run_inventory_event_staging
        from medwarehouse.spark.jobs.procurement_bronze import run_procurement_bronze
        from medwarehouse.spark.jobs.procurement_silver import run_procurement_silver
        from medwarehouse.spark.jobs.procurement_stage import run_procurement_event_staging
        from medwarehouse.spark.jobs.sales_bronze import run_sales_bronze
        from medwarehouse.spark.jobs.sales_silver import run_sales_silver
        from medwarehouse.spark.jobs.sales_stage import run_sales_event_staging

        if args.spark_job == "inventory-bronze":
            run_inventory_bronze(starting_offsets=args.starting_offsets)
        elif args.spark_job == "inventory-silver":
            run_inventory_silver()
        elif args.spark_job == "stage-inventory-events":
            run_inventory_event_staging()
        elif args.spark_job == "procurement-bronze":
            run_procurement_bronze(starting_offsets=args.starting_offsets)
        elif args.spark_job == "procurement-silver":
            run_procurement_silver()
        elif args.spark_job == "stage-procurement-events":
            run_procurement_event_staging()
        elif args.spark_job == "sales-bronze":
            run_sales_bronze(starting_offsets=args.starting_offsets)
        elif args.spark_job == "sales-silver":
            run_sales_silver()
        elif args.spark_job == "stage-sales-events":
            run_sales_event_staging()
        return 0

    if args.command == "warehouse":
        from medwarehouse.warehouse.inventory import (
            bootstrap_warehouse, build_inventory_gold, check_reorder_thresholds,
            load_inventory_event_facts, refresh_inventory_balance,
            refresh_inventory_dimensions, refresh_inventory_semantic_views,
            run_inventory_quality_checks,
        )
        from medwarehouse.warehouse.procurement import (
            build_procurement_gold, load_procurement_event_facts,
            refresh_procurement_semantic_views, run_procurement_quality_checks,
        )
        from medwarehouse.warehouse.sales import (
            build_sales_gold, load_sales_event_facts, refresh_sales_dimensions,
            refresh_sales_semantic_views, run_sales_quality_checks,
        )

        _warehouse_dispatch = {
            "bootstrap":                    bootstrap_warehouse,
            "refresh-dimensions":           refresh_inventory_dimensions,
            "load-facts":                   load_inventory_event_facts,
            "refresh-views":                refresh_inventory_semantic_views,
            "quality-checks":               run_inventory_quality_checks,
            "refresh-balance":              refresh_inventory_balance,
            "check-reorders":               check_reorder_thresholds,
            "inventory-gold":               build_inventory_gold,
            "load-procurement-facts":       load_procurement_event_facts,
            "refresh-procurement-views":    refresh_procurement_semantic_views,
            "procurement-quality-checks":   run_procurement_quality_checks,
            "procurement-gold":             build_procurement_gold,
            "refresh-sales-dimensions":     refresh_sales_dimensions,
            "load-sales-facts":             load_sales_event_facts,
            "refresh-sales-views":          refresh_sales_semantic_views,
            "sales-quality-checks":         run_sales_quality_checks,
            "sales-gold":                   build_sales_gold,
        }
        _warehouse_dispatch[args.warehouse_command]()
        return 0

    if args.command == "orchestration":
        from medwarehouse.spark.jobs.inventory_stage import run_inventory_event_staging
        from medwarehouse.spark.jobs.procurement_stage import run_procurement_event_staging
        from medwarehouse.spark.jobs.sales_stage import run_sales_event_staging
        from medwarehouse.warehouse.inventory import (
            build_inventory_gold,
            validate_inventory_silver_ready,
        )
        from medwarehouse.warehouse.procurement import (
            build_procurement_gold,
            validate_procurement_silver_ready,
        )
        from medwarehouse.warehouse.sales import (
            build_sales_gold,
            validate_sales_silver_ready,
        )

        if args.orchestration_command == "validate-silver":
            validate_inventory_silver_ready()
        elif args.orchestration_command == "stage-events":
            validate_inventory_silver_ready()
            run_inventory_event_staging()
        elif args.orchestration_command == "build-gold":
            validate_inventory_silver_ready()
            run_inventory_event_staging()
            build_inventory_gold()
        elif args.orchestration_command == "validate-procurement-silver":
            validate_procurement_silver_ready()
        elif args.orchestration_command == "stage-procurement-events":
            validate_procurement_silver_ready()
            run_procurement_event_staging()
        elif args.orchestration_command == "build-procurement-gold":
            validate_procurement_silver_ready()
            run_procurement_event_staging()
            build_procurement_gold()
        elif args.orchestration_command == "validate-sales-silver":
            validate_sales_silver_ready()
        elif args.orchestration_command == "stage-sales-events":
            validate_sales_silver_ready()
            run_sales_event_staging()
        elif args.orchestration_command == "build-sales-gold":
            validate_sales_silver_ready()
            run_sales_event_staging()
            build_sales_gold()
        return 0

    if args.command == "platform":
        if args.platform_command == "serve":
            run_platform_server(host=args.host, port=args.port, debug=args.debug)
        return 0

    parser.error("Unhandled command")
    return 1
