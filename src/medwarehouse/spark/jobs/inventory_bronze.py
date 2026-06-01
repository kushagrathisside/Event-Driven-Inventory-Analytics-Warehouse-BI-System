from __future__ import annotations

from pyspark.sql.functions import col
from pyspark.sql.types import StringType

from medwarehouse.config import get_settings
from medwarehouse.logging import get_logger
from medwarehouse.spark.session import build_spark_session


logger = get_logger(__name__)


def run_inventory_bronze(*, starting_offsets: str = "earliest") -> None:
    settings = get_settings()
    settings.paths.bronze_inventory_path.parent.mkdir(parents=True, exist_ok=True)
    settings.paths.inventory_checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

    spark = build_spark_session("InventoryBronzeIngestion")
    logger.info(
        "Starting bronze stream topic=%s output=%s checkpoint=%s",
        settings.kafka.inventory_topic,
        settings.paths.bronze_inventory_path,
        settings.paths.inventory_checkpoint_path,
    )

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka.bootstrap_servers)
        .option("subscribe", settings.kafka.inventory_topic)
        .option("startingOffsets", starting_offsets)
        .load()
    )

    raw = df.select(
        col("key").cast(StringType()).alias("kafka_key"),
        col("value").cast(StringType()).alias("raw_event"),
        col("topic").alias("source_topic"),
        col("partition").alias("source_partition"),
        col("offset").alias("source_offset"),
        col("timestamp").alias("source_kafka_timestamp"),
    )

    query = (
        raw.writeStream.format("parquet")
        .option("path", str(settings.paths.bronze_inventory_path))
        .option("checkpointLocation", str(settings.paths.inventory_checkpoint_path))
        .outputMode("append")
        .start()
    )

    query.awaitTermination()
