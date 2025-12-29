# ============================================================
# Inventory Balance - GOLD BASE
# Purpose:
#   - Consume Silver inventory balance
#   - Enforce referential integrity against master.products
#   - Write clean, canonical Gold Base dataset
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, max as _max

# ------------------------------------------------------------
# Spark Session
# ------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("InventoryBalanceGoldBase")
    .getOrCreate()
)

# ------------------------------------------------------------
# Config
# ------------------------------------------------------------
SILVER_PATH = "data/silver/inventory_events"
GOLD_PATH   = "data/gold/inventory_balance"

jdbc_url = "jdbc:postgresql://localhost:5432/medwarehouse_analytics"

jdbc_props = {
    "user": "spark_reader",
    "password": "spark_reader_pwd",
    "driver": "org.postgresql.Driver"
}

# ------------------------------------------------------------
# Read Silver Inventory Events
# ------------------------------------------------------------
events = spark.read.parquet(SILVER_PATH)

# Expected schema (important):
# product_id, warehouse_id, batch_number,
# expiry_date, quantity_delta, event_time

# ------------------------------------------------------------
# Aggregate to Inventory Balance
# ------------------------------------------------------------
inventory_balance = (
    events
    .groupBy(
        "product_id",
        "warehouse_id",
        "batch_number",
        "expiry_date"
    )
    .agg(
        _sum("quantity_delta").alias("current_quantity"),
        _max("event_time").alias("last_event_time")
    )
)

# ------------------------------------------------------------
# 🔑 STEP 1 FIX: Load authoritative Product Dimension
# ------------------------------------------------------------
dim_product = (
    spark.read
    .jdbc(
        url=jdbc_url,
        table="master.products",
        properties=jdbc_props
    )
    .select("product_id")
    .distinct()
)

# ------------------------------------------------------------
# 🔑 STEP 1 FIX: Enforce Referential Integrity
#   - Drops P-001 and any invalid IDs automatically
# ------------------------------------------------------------
inventory_balance_clean = (
    inventory_balance
    .join(
        dim_product,
        on="product_id",
        how="inner"   # ← THIS is the enforcement
    )
)

# ------------------------------------------------------------
# Write Gold Base (overwrite is REQUIRED)
# ------------------------------------------------------------
(
    inventory_balance_clean
    .write
    .mode("overwrite")
    .parquet(GOLD_PATH)
)

spark.stop()

