from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from medwarehouse.config import get_settings
from medwarehouse.logging import get_logger
from medwarehouse.spark.jobs._silver import split_silver_quarantine
from medwarehouse.spark.session import build_spark_session


logger = get_logger(__name__)


PAYLOAD_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), True),
        StructField("warehouse_id", StringType(), True),
        StructField("batch_number", StringType(), True),
        StructField("expiry_date", StringType(), True),
        StructField("quantity_delta", IntegerType(), True),
        StructField("supplier_id", StringType(), True),
        StructField("sale_id", StringType(), True),
        StructField("reason", StringType(), True),
    ]
)

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("producer", StringType(), True),
        StructField("schema_version", IntegerType(), True),
        StructField("payload", PAYLOAD_SCHEMA, True),
    ]
)


def _build_validation_columns(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "validation_errors_raw",
            F.array(
                F.when(F.col("event_id").isNull(), F.lit("missing_event_id")),
                F.when(F.col("event_type").isNull(), F.lit("missing_event_type")),
                F.when(F.col("event_time_ts").isNull(), F.lit("invalid_event_time")),
                F.when(F.col("producer").isNull(), F.lit("missing_producer")),
                F.when(
                    ~F.col("event_type").isin(
                        "STOCK_RECEIVED",
                        "STOCK_SOLD",
                        "STOCK_ADJUSTED",
                        "STOCK_EXPIRED",
                    ),
                    F.lit("unsupported_event_type"),
                ),
                F.when(F.col("product_id").isNull(), F.lit("missing_product_id")),
                F.when(F.col("warehouse_id").isNull(), F.lit("missing_warehouse_id")),
                F.when(F.col("batch_number").isNull(), F.lit("missing_batch_number")),
                F.when(F.col("quantity_delta").isNull(), F.lit("missing_quantity_delta")),
                F.when(
                    F.col("event_type").isin("STOCK_RECEIVED", "STOCK_SOLD", "STOCK_EXPIRED")
                    & F.col("expiry_date").isNull(),
                    F.lit("missing_expiry_date"),
                ),
                F.when(
                    (F.col("event_type") == "STOCK_RECEIVED") & (F.col("quantity_delta") <= 0),
                    F.lit("received_quantity_must_be_positive"),
                ),
                F.when(
                    F.col("event_type").isin("STOCK_SOLD", "STOCK_EXPIRED")
                    & (F.col("quantity_delta") >= 0),
                    F.lit("decrement_event_must_be_negative"),
                ),
                F.when(
                    (F.col("event_type") == "STOCK_ADJUSTED") & (F.col("quantity_delta") == 0),
                    F.lit("adjustment_quantity_must_be_non_zero"),
                ),
                F.when(
                    (F.col("event_type") == "STOCK_ADJUSTED") & F.col("adjustment_reason").isNull(),
                    F.lit("missing_adjustment_reason"),
                ),
                F.when(
                    (F.col("event_type") == "STOCK_SOLD") & F.col("sale_id").isNull(),
                    F.lit("missing_sale_id"),
                ),
            ),
        )
        .withColumn(
            "validation_errors",
            F.expr("filter(validation_errors_raw, x -> x is not null)"),
        )
    )


def _normalize_inventory_events(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    parsed = (
        bronze_df.select(
            F.col("raw_event"),
            F.col("kafka_key"),
            F.col("source_topic"),
            F.col("source_partition"),
            F.col("source_offset"),
            F.col("source_kafka_timestamp"),
            F.from_json(F.col("raw_event"), EVENT_SCHEMA).alias("event"),
        )
        .select(
            F.col("event.event_id").alias("event_id"),
            F.col("event.event_type").alias("event_type"),
            F.to_timestamp(F.col("event.event_time")).alias("event_time_ts"),
            F.col("event.producer").alias("producer"),
            F.col("event.schema_version").alias("schema_version"),
            F.col("event.payload.product_id").alias("product_id"),
            F.col("event.payload.warehouse_id").alias("warehouse_id"),
            F.col("event.payload.batch_number").alias("batch_number"),
            F.to_date(F.col("event.payload.expiry_date")).alias("expiry_date"),
            F.col("event.payload.quantity_delta").alias("quantity_delta"),
            F.col("event.payload.supplier_id").alias("supplier_id"),
            F.col("event.payload.sale_id").alias("sale_id"),
            F.col("event.payload.reason").alias("adjustment_reason"),
            F.col("raw_event"),
            F.col("kafka_key"),
            F.col("source_topic"),
            F.col("source_partition"),
            F.col("source_offset"),
            F.col("source_kafka_timestamp"),
        )
        .withColumn("event_date", F.to_date(F.col("event_time_ts")))
        .withColumn("dedupe_key", F.coalesce(F.col("event_id"), F.sha2(F.col("raw_event"), 256)))
    )
    return split_silver_quarantine(parsed, validation_builder=_build_validation_columns)


def run_inventory_silver() -> dict[str, int]:
    settings = get_settings()
    settings.paths.silver_inventory_path.parent.mkdir(parents=True, exist_ok=True)
    settings.paths.quarantine_inventory_path.parent.mkdir(parents=True, exist_ok=True)

    spark = build_spark_session("InventorySilverTransformation")
    bronze_df = spark.read.parquet(str(settings.paths.bronze_inventory_path))
    silver_df, quarantine_df = _normalize_inventory_events(bronze_df)

    silver_df.write.mode("overwrite").parquet(str(settings.paths.silver_inventory_path))
    quarantine_df.write.mode("overwrite").parquet(str(settings.paths.quarantine_inventory_path))

    result = {
        "bronze_rows": bronze_df.count(),
        "silver_rows": silver_df.count(),
        "quarantine_rows": quarantine_df.count(),
    }
    logger.info("Silver refresh complete %s", result)
    spark.stop()
    return result
