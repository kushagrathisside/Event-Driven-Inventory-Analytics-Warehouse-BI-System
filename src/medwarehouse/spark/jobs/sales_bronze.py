from __future__ import annotations

from medwarehouse.config import get_settings
from medwarehouse.spark.jobs._bronze import run_bronze_ingestion


def run_sales_bronze(*, starting_offsets: str = "earliest") -> None:
    settings = get_settings()
    run_bronze_ingestion(
        app_name="SalesBronzeIngestion",
        bootstrap_servers=settings.kafka.bootstrap_servers,
        topic=settings.kafka.sales_topic,
        output_path=settings.paths.bronze_sales_path,
        checkpoint_path=settings.paths.sales_checkpoint_path,
        starting_offsets=starting_offsets,
    )
