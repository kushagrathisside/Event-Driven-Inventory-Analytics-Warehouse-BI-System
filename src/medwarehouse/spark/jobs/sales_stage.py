from __future__ import annotations

from pyspark.sql import functions as F

from medwarehouse.config import get_settings
from medwarehouse.logging import get_logger
from medwarehouse.spark.jobs._stage import write_to_staging
from medwarehouse.spark.session import build_spark_session


logger = get_logger(__name__)


def run_sales_event_staging() -> int:
    settings = get_settings()
    spark = build_spark_session("SalesEventWarehouseStaging")
    silver_df = spark.read.parquet(str(settings.paths.silver_sales_path))

    staged = silver_df.select(
        "event_id",
        "event_type",
        F.col("event_time").cast("timestamp").alias("event_time"),
        "event_date",
        "producer",
        "schema_version",
        "sale_id",
        "product_id",
        "warehouse_id",
        "quantity",
        F.col("unit_price").cast("decimal(10,2)").alias("unit_price"),
        "currency",
        "customer_type",
        "original_event_id",
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
        table="analytics.stg_sales_events",
        domain_label="sales",
    )
