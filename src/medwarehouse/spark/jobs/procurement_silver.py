from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, StringType, StructField, StructType,
)

from medwarehouse.config import get_settings
from medwarehouse.logging import get_logger
from medwarehouse.spark.jobs._silver import split_silver_quarantine
from medwarehouse.spark.session import build_spark_session


logger = get_logger(__name__)


PROCUREMENT_PAYLOAD_SCHEMA = StructType([
    StructField("po_id",             StringType(),  True),
    StructField("product_id",        StringType(),  True),
    StructField("ordered_quantity",  IntegerType(), True),
    StructField("supplier_id",       StringType(),  True),
    StructField("trigger_reason",    StringType(),  True),
    StructField("approved_by",       StringType(),  True),
    StructField("received_quantity", IntegerType(), True),
    StructField("warehouse_id",      StringType(),  True),
    StructField("reason",            StringType(),  True),
])

PROCUREMENT_EVENT_SCHEMA = StructType([
    StructField("event_id",       StringType(),  True),
    StructField("event_type",     StringType(),  True),
    StructField("event_time",     StringType(),  True),
    StructField("producer",       StringType(),  True),
    StructField("schema_version", IntegerType(), True),
    StructField("payload",        PROCUREMENT_PAYLOAD_SCHEMA, True),
])

_VALID_TYPES = ("PO_CREATED", "PO_APPROVED", "PO_RECEIVED", "PO_CANCELLED")
_VALID_TRIGGER_REASONS = ("AUTO_REORDER", "MANUAL")


def _build_validation_columns(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "validation_errors_raw",
            F.array(
                F.when(F.col("event_id").isNull(),   F.lit("missing_event_id")),
                F.when(F.col("event_type").isNull(), F.lit("missing_event_type")),
                F.when(F.col("event_time_ts").isNull(), F.lit("invalid_event_time")),
                F.when(F.col("producer").isNull(),   F.lit("missing_producer")),
                F.when(~F.col("event_type").isin(*_VALID_TYPES), F.lit("unsupported_event_type")),
                # PO_CREATED required fields
                F.when(
                    (F.col("event_type") == "PO_CREATED") & F.col("po_id").isNull(),
                    F.lit("po_created_missing_po_id"),
                ),
                F.when(
                    (F.col("event_type") == "PO_CREATED") & F.col("product_id").isNull(),
                    F.lit("po_created_missing_product_id"),
                ),
                F.when(
                    (F.col("event_type") == "PO_CREATED")
                    & (F.col("ordered_quantity").isNull() | (F.col("ordered_quantity") <= 0)),
                    F.lit("po_created_invalid_ordered_quantity"),
                ),
                F.when(
                    (F.col("event_type") == "PO_CREATED") & F.col("supplier_id").isNull(),
                    F.lit("po_created_missing_supplier_id"),
                ),
                F.when(
                    (F.col("event_type") == "PO_CREATED")
                    & ~F.col("trigger_reason").isin(*_VALID_TRIGGER_REASONS),
                    F.lit("po_created_invalid_trigger_reason"),
                ),
                # PO_APPROVED required fields
                F.when(
                    (F.col("event_type") == "PO_APPROVED") & F.col("po_id").isNull(),
                    F.lit("po_approved_missing_po_id"),
                ),
                F.when(
                    (F.col("event_type") == "PO_APPROVED") & F.col("approved_by").isNull(),
                    F.lit("po_approved_missing_approved_by"),
                ),
                # PO_RECEIVED required fields
                F.when(
                    (F.col("event_type") == "PO_RECEIVED") & F.col("po_id").isNull(),
                    F.lit("po_received_missing_po_id"),
                ),
                F.when(
                    (F.col("event_type") == "PO_RECEIVED") & F.col("product_id").isNull(),
                    F.lit("po_received_missing_product_id"),
                ),
                F.when(
                    (F.col("event_type") == "PO_RECEIVED")
                    & (F.col("received_quantity").isNull() | (F.col("received_quantity") <= 0)),
                    F.lit("po_received_invalid_received_quantity"),
                ),
                F.when(
                    (F.col("event_type") == "PO_RECEIVED") & F.col("warehouse_id").isNull(),
                    F.lit("po_received_missing_warehouse_id"),
                ),
                # PO_CANCELLED required fields
                F.when(
                    (F.col("event_type") == "PO_CANCELLED") & F.col("po_id").isNull(),
                    F.lit("po_cancelled_missing_po_id"),
                ),
                F.when(
                    (F.col("event_type") == "PO_CANCELLED") & F.col("reason").isNull(),
                    F.lit("po_cancelled_missing_reason"),
                ),
            ),
        )
        .withColumn(
            "validation_errors",
            F.expr("filter(validation_errors_raw, x -> x is not null)"),
        )
    )


def _normalize_procurement_events(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:  # noqa: ignore
    parsed = (
        bronze_df.select(
            F.col("raw_event"),
            F.col("kafka_key"),
            F.col("source_topic"),
            F.col("source_partition"),
            F.col("source_offset"),
            F.col("source_kafka_timestamp"),
            F.from_json(F.col("raw_event"), PROCUREMENT_EVENT_SCHEMA).alias("event"),
        )
        .select(
            F.col("event.event_id").alias("event_id"),
            F.col("event.event_type").alias("event_type"),
            F.to_timestamp(F.col("event.event_time")).alias("event_time_ts"),
            F.col("event.producer").alias("producer"),
            F.col("event.schema_version").alias("schema_version"),
            F.col("event.payload.po_id").alias("po_id"),
            F.col("event.payload.product_id").alias("product_id"),
            F.col("event.payload.ordered_quantity").alias("ordered_quantity"),
            F.col("event.payload.supplier_id").alias("supplier_id"),
            F.col("event.payload.trigger_reason").alias("trigger_reason"),
            F.col("event.payload.approved_by").alias("approved_by"),
            F.col("event.payload.received_quantity").alias("received_quantity"),
            F.col("event.payload.warehouse_id").alias("warehouse_id"),
            F.col("event.payload.reason").alias("reason"),
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


def run_procurement_silver() -> dict[str, int]:
    settings = get_settings()
    settings.paths.silver_procurement_path.parent.mkdir(parents=True, exist_ok=True)
    settings.paths.quarantine_procurement_path.parent.mkdir(parents=True, exist_ok=True)

    spark = build_spark_session("ProcurementSilverTransformation")
    bronze_df = spark.read.parquet(str(settings.paths.bronze_procurement_path))
    silver_df, quarantine_df = _normalize_procurement_events(bronze_df)

    silver_df.write.mode("overwrite").parquet(str(settings.paths.silver_procurement_path))
    quarantine_df.write.mode("overwrite").parquet(str(settings.paths.quarantine_procurement_path))

    result = {
        "bronze_rows": bronze_df.count(),
        "silver_rows": silver_df.count(),
        "quarantine_rows": quarantine_df.count(),
    }
    logger.info("Procurement silver refresh complete %s", result)
    spark.stop()
    return result
