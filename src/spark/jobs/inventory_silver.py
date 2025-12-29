from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType
)

# -----------------------------
# Spark Session
# -----------------------------
spark = (
    SparkSession.builder
    .appName("InventorySilverTransformation")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# Bronze Event Schema
# -----------------------------
payload_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("warehouse_id", StringType(), True),
    StructField("batch_number", StringType(), True),
    StructField("expiry_date", StringType(), True),
    StructField("quantity_delta", IntegerType(), True),
    StructField("supplier_id", StringType(), True)
])

event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("producer", StringType(), True),
    StructField("schema_version", IntegerType(), True),
    StructField("payload", payload_schema, True)
])

# -----------------------------
# Read Bronze
# -----------------------------
bronze_df = (
    spark.read
    .parquet("data/bronze/inventory_events")
)

# -----------------------------
# Parse & Normalize
# -----------------------------
silver_df = (
    bronze_df
    .select(
        from_json(col("raw_event"), event_schema).alias("event"),
        col("kafka_timestamp")
    )
    .select(
        col("event.event_id"),
        col("event.event_type"),
        to_timestamp(col("event.event_time")).alias("event_time"),
        col("event.producer"),
        col("event.schema_version"),

        col("event.payload.product_id"),
        col("event.payload.warehouse_id"),
        col("event.payload.batch_number"),
        col("event.payload.expiry_date"),
        col("event.payload.quantity_delta"),
        col("event.payload.supplier_id"),

        col("kafka_timestamp")
    )
    # Minimal domain filter
    .filter(col("product_id").isNotNull())
)

# -----------------------------
# Write Silver
# -----------------------------
(
    silver_df
    .write
    .mode("append")
    .parquet("data/silver/inventory_events")
)

