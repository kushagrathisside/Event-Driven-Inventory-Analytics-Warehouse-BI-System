from __future__ import annotations

from pyspark.sql import functions as F

from medwarehouse.config import get_settings
from medwarehouse.logging import get_logger
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

    row_count = staged.count()
    logger.info(
        "Writing inventory staging rows=%s jdbc=%s table=analytics.stg_inventory_events",
        row_count,
        settings.analytics_writer_db.jdbc_url,
    )
    (
        staged.write.format("jdbc")
        .option("url", settings.analytics_writer_db.jdbc_url)
        .option("dbtable", "analytics.stg_inventory_events")
        .option("user", settings.analytics_writer_db.user)
        .option("password", settings.analytics_writer_db.password)
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )
    spark.stop()
    return row_count
