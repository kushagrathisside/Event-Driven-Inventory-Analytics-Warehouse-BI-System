from __future__ import annotations

import argparse

from medwarehouse.config import get_settings
from medwarehouse.logging import configure_logging
from medwarehouse.platform import run_platform_server
from medwarehouse.producers import (
    run_inventory_producer,
    run_procurement_producer,
    run_sales_producer,
)
from medwarehouse.spark.jobs import (
    run_inventory_bronze,
    run_inventory_event_staging,
    run_inventory_silver,
)
from medwarehouse.warehouse.inventory import (
    bootstrap_warehouse,
    build_inventory_gold,
    load_inventory_event_facts,
    refresh_inventory_dimensions,
    refresh_inventory_semantic_views,
    run_inventory_quality_checks,
    validate_inventory_silver_ready,
)


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
    bronze = spark_sub.add_parser("inventory-bronze")
    bronze.add_argument("--starting-offsets", default="earliest")
    spark_sub.add_parser("inventory-silver")
    spark_sub.add_parser("stage-inventory-events")

    warehouse = subparsers.add_parser("warehouse")
    warehouse_sub = warehouse.add_subparsers(dest="warehouse_command", required=True)
    warehouse_sub.add_parser("bootstrap")
    warehouse_sub.add_parser("refresh-dimensions")
    warehouse_sub.add_parser("load-facts")
    warehouse_sub.add_parser("refresh-views")
    warehouse_sub.add_parser("quality-checks")
    warehouse_sub.add_parser("inventory-gold")

    orchestration = subparsers.add_parser("orchestration")
    orchestration_sub = orchestration.add_subparsers(
        dest="orchestration_command", required=True
    )
    orchestration_sub.add_parser("validate-silver")
    orchestration_sub.add_parser("stage-events")
    orchestration_sub.add_parser("build-gold")

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
        if args.producer_name == "inventory":
            run_inventory_producer(max_events=args.max_events, dry_run=args.dry_run)
        elif args.producer_name == "procurement":
            run_procurement_producer(max_events=args.max_events, dry_run=args.dry_run)
        elif args.producer_name == "sales":
            run_sales_producer(max_events=args.max_events, dry_run=args.dry_run)
        return 0

    if args.command == "spark":
        if args.spark_job == "inventory-bronze":
            run_inventory_bronze(starting_offsets=args.starting_offsets)
        elif args.spark_job == "inventory-silver":
            run_inventory_silver()
        elif args.spark_job == "stage-inventory-events":
            run_inventory_event_staging()
        return 0

    if args.command == "warehouse":
        if args.warehouse_command == "bootstrap":
            bootstrap_warehouse()
        elif args.warehouse_command == "refresh-dimensions":
            refresh_inventory_dimensions()
        elif args.warehouse_command == "load-facts":
            load_inventory_event_facts()
        elif args.warehouse_command == "refresh-views":
            refresh_inventory_semantic_views()
        elif args.warehouse_command == "quality-checks":
            run_inventory_quality_checks()
        elif args.warehouse_command == "inventory-gold":
            build_inventory_gold()
        return 0

    if args.command == "orchestration":
        if args.orchestration_command == "validate-silver":
            validate_inventory_silver_ready()
        elif args.orchestration_command == "stage-events":
            validate_inventory_silver_ready()
            run_inventory_event_staging()
        elif args.orchestration_command == "build-gold":
            validate_inventory_silver_ready()
            run_inventory_event_staging()
            build_inventory_gold()
        return 0

    if args.command == "platform":
        if args.platform_command == "serve":
            run_platform_server(host=args.host, port=args.port, debug=args.debug)
        return 0

    parser.error("Unhandled command")
    return 1
