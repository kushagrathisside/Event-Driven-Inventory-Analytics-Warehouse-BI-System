from __future__ import annotations

from pyspark.sql import functions as F

from medwarehouse.config import get_settings
from medwarehouse.logging import get_logger
from medwarehouse.spark.jobs._stage import write_to_staging
from medwarehouse.spark.session import build_spark_session


logger = get_logger(__name__)


def run_procurement_event_staging() -> int:
    settings = get_settings()
    spark = build_spark_session("ProcurementEventWarehouseStaging")
    silver_df = spark.read.parquet(str(settings.paths.silver_procurement_path))

    staged = silver_df.select(
        "event_id",
        "event_type",
        F.col("event_time").cast("timestamp").alias("event_time"),
        "event_date",
        "producer",
        "schema_version",
        "po_id",
        "product_id",
        "ordered_quantity",
        "supplier_id",
        "trigger_reason",
        "approved_by",
        "received_quantity",
        "warehouse_id",
        "reason",
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
        table="analytics.stg_procurement_events",
        domain_label="procurement",
    )
