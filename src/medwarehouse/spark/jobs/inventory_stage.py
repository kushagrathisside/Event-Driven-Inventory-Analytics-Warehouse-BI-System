from __future__ import annotations

from pyspark.sql import functions as F

from medwarehouse.config import get_settings
from medwarehouse.logging import get_logger
from medwarehouse.spark.jobs._stage import write_to_staging
from medwarehouse.spark.session import build_spark_session


logger = get_logger(__name__)


def run_inventory_event_staging() -> int:
    settings = get_settings()
    spark = build_spark_session("InventoryEventWarehouseStaging")
    silver_df = spark.read.parquet(str(settings.paths.silver_inventory_path))

    staged = silver_df.select(
        "event_id",
        "event_type",
        F.col("event_time").cast("timestamp").alias("event_time"),
        "event_date",
        "producer",
        "schema_version",
        "product_id",
        "warehouse_id",
        "batch_number",
        F.col("expiry_date").cast("date").alias("expiry_date"),
        "quantity_delta",
        "supplier_id",
        "sale_id",
        "adjustment_reason",
        "kafka_key",
        "source_topic",
        "source_partition",
        "source_offset",
        F.col("source_kafka_timestamp").cast("timestamp").alias("source_kafka_timestamp"),
        "raw_event",
    )

    return write_to_staging(
        spark, staged,
        connection=settings.analytics_writer_db,
        table="analytics.stg_inventory_events",
        domain_label="inventory",
    )
