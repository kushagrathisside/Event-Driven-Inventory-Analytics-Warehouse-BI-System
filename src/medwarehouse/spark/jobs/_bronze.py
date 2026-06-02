"""Shared Bronze ingestion logic for all Kafka domains."""
from __future__ import annotations

from pathlib import Path

from pyspark.sql.functions import col
from pyspark.sql.types import StringType

from medwarehouse.logging import get_logger
from medwarehouse.spark.session import build_spark_session


logger = get_logger(__name__)


def run_bronze_ingestion(
    *,
    app_name: str,
    bootstrap_servers: str,
    topic: str,
    output_path: Path,
    checkpoint_path: Path,
    starting_offsets: str = "earliest",
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

    spark = build_spark_session(app_name)
    logger.info(
        "Starting bronze stream app=%s topic=%s output=%s checkpoint=%s",
        app_name, topic, output_path, checkpoint_path,
    )

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
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
        .option("path", str(output_path))
        .option("checkpointLocation", str(checkpoint_path))
        .outputMode("append")
        .start()
    )
    query.awaitTermination()
