from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType, IntegerType, StringType, StructField, StructType,
)

from medwarehouse.config import get_settings
from medwarehouse.logging import get_logger
from medwarehouse.spark.jobs._silver import split_silver_quarantine
from medwarehouse.spark.session import build_spark_session


logger = get_logger(__name__)


SALES_PAYLOAD_SCHEMA = StructType([
    StructField("sale_id",           StringType(),       True),
    StructField("product_id",        StringType(),       True),
    StructField("warehouse_id",      StringType(),       True),
    StructField("quantity",          IntegerType(),      True),
    StructField("unit_price",        DecimalType(10, 2), True),
    StructField("currency",          StringType(),       True),
    StructField("customer_type",     StringType(),       True),
    StructField("original_event_id", StringType(),       True),
])

SALES_EVENT_SCHEMA = StructType([
    StructField("event_id",       StringType(),  True),
    StructField("event_type",     StringType(),  True),
    StructField("event_time",     StringType(),  True),
    StructField("producer",       StringType(),  True),
    StructField("schema_version", IntegerType(), True),
    StructField("payload",        SALES_PAYLOAD_SCHEMA, True),
])

_VALID_TYPES = ("SALE_CREATED", "SALE_CANCELLED")
_VALID_CUSTOMER_TYPES = ("RETAIL", "HOSPITAL", "DISTRIBUTOR")


def _build_validation_columns(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "validation_errors_raw",
            F.array(
                F.when(F.col("event_id").isNull(),      F.lit("missing_event_id")),
                F.when(F.col("event_type").isNull(),    F.lit("missing_event_type")),
                F.when(F.col("event_time_ts").isNull(), F.lit("invalid_event_time")),
                F.when(F.col("producer").isNull(),      F.lit("missing_producer")),
                F.when(~F.col("event_type").isin(*_VALID_TYPES), F.lit("unsupported_event_type")),
                # Both event types require these
                F.when(F.col("sale_id").isNull(),      F.lit("missing_sale_id")),
                F.when(F.col("product_id").isNull(),   F.lit("missing_product_id")),
                F.when(F.col("warehouse_id").isNull(), F.lit("missing_warehouse_id")),
                F.when(
                    F.col("quantity").isNull() | (F.col("quantity") <= 0),
                    F.lit("quantity_must_be_positive"),
                ),
                # SALE_CREATED specific
                F.when(
                    (F.col("event_type") == "SALE_CREATED") & F.col("unit_price").isNull(),
                    F.lit("sale_created_missing_unit_price"),
                ),
                F.when(
                    (F.col("event_type") == "SALE_CREATED")
                    & (F.col("unit_price").isNotNull())
                    & (F.col("unit_price") <= 0),
                    F.lit("sale_created_unit_price_must_be_positive"),
                ),
                F.when(
                    (F.col("event_type") == "SALE_CREATED") & F.col("currency").isNull(),
                    F.lit("sale_created_missing_currency"),
                ),
                F.when(
                    (F.col("event_type") == "SALE_CREATED")
                    & ~F.col("customer_type").isin(*_VALID_CUSTOMER_TYPES),
                    F.lit("sale_created_invalid_customer_type"),
                ),
                # SALE_CANCELLED specific
                F.when(
                    (F.col("event_type") == "SALE_CANCELLED") & F.col("original_event_id").isNull(),
                    F.lit("sale_cancelled_missing_original_event_id"),
                ),
            ),
        )
        .withColumn(
            "validation_errors",
            F.expr("filter(validation_errors_raw, x -> x is not null)"),
        )
    )


def _normalize_sales_events(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    parsed = (
        bronze_df.select(
            F.col("raw_event"),
            F.col("kafka_key"),
            F.col("source_topic"),
            F.col("source_partition"),
            F.col("source_offset"),
            F.col("source_kafka_timestamp"),
            F.from_json(F.col("raw_event"), SALES_EVENT_SCHEMA).alias("event"),
        )
        .select(
            F.col("event.event_id").alias("event_id"),
            F.col("event.event_type").alias("event_type"),
            F.to_timestamp(F.col("event.event_time")).alias("event_time_ts"),
            F.col("event.producer").alias("producer"),
            F.col("event.schema_version").alias("schema_version"),
            F.col("event.payload.sale_id").alias("sale_id"),
            F.col("event.payload.product_id").alias("product_id"),
            F.col("event.payload.warehouse_id").alias("warehouse_id"),
            F.col("event.payload.quantity").alias("quantity"),
            F.col("event.payload.unit_price").alias("unit_price"),
            F.col("event.payload.currency").alias("currency"),
            F.col("event.payload.customer_type").alias("customer_type"),
            F.col("event.payload.original_event_id").alias("original_event_id"),
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


def run_sales_silver() -> dict[str, int]:
    settings = get_settings()
    settings.paths.silver_sales_path.parent.mkdir(parents=True, exist_ok=True)
    settings.paths.quarantine_sales_path.parent.mkdir(parents=True, exist_ok=True)

    spark = build_spark_session("SalesSilverTransformation")
    bronze_df = spark.read.parquet(str(settings.paths.bronze_sales_path))
    silver_df, quarantine_df = _normalize_sales_events(bronze_df)

    silver_df.write.mode("overwrite").parquet(str(settings.paths.silver_sales_path))
    quarantine_df.write.mode("overwrite").parquet(str(settings.paths.quarantine_sales_path))

    result = {
        "bronze_rows": bronze_df.count(),
        "silver_rows": silver_df.count(),
        "quarantine_rows": quarantine_df.count(),
    }
    logger.info("Sales silver refresh complete %s", result)
    spark.stop()
    return result
