from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)

spark = (
    SparkSession.builder
    .appName("InventorySilverTransformation")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# Schema
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
bronze_df = spark.read.parquet(
    "data/bronze/inventory_events"
)

# -----------------------------
# Parse JSON
# -----------------------------
silver_df = (
    bronze_df
    .select(from_json(col("raw_event"), event_schema).alias("data"))
    .select("data.*", "data.payload.*")
    .drop("payload")
)

# -----------------------------
# Write Silver
# -----------------------------
(
    silver_df
    .write
    .mode("overwrite")
    .parquet("data/silver/inventory_events")
)

spark.stop()

