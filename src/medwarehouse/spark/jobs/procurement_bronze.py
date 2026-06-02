from __future__ import annotations

from medwarehouse.config import get_settings
from medwarehouse.spark.jobs._bronze import run_bronze_ingestion


def run_procurement_bronze(*, starting_offsets: str = "earliest") -> None:
    settings = get_settings()
    run_bronze_ingestion(
        app_name="ProcurementBronzeIngestion",
        bootstrap_servers=settings.kafka.bootstrap_servers,
        topic=settings.kafka.procurement_topic,
        output_path=settings.paths.bronze_procurement_path,
        checkpoint_path=settings.paths.procurement_checkpoint_path,
        starting_offsets=starting_offsets,
    )
