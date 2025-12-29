from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp

# ============================================================
# Spark Session
# ============================================================
spark = (
    SparkSession.builder
    .appName("LoadInventoryBalanceToPostgres")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# Paths
# ============================================================
GOLD_PATH = "data/gold/inventory_balance_enriched"

# ============================================================
# PostgreSQL JDBC Configuration
# ============================================================
jdbc_url = "jdbc:postgresql://localhost:5432/medwarehouse_analytics"

jdbc_props = {
    "user": "spark_reader",
    "password": "spark_reader_pwd",
    "driver": "org.postgresql.Driver"
}

# ============================================================
# Read Gold Inventory Balance (Already Aggregated)
# ============================================================
df = spark.read.parquet(GOLD_PATH)

# ============================================================
# Select ONLY columns that exist in Postgres table
# ============================================================
df_final = (
    df
    .withColumn("expiry_date", to_date(col("expiry_date")))
    .withColumn("last_event_time", to_timestamp(col("last_event_time")))
    .select(
        "product_id",
        "warehouse_id",
        "batch_number",
        "expiry_date",
        "current_quantity",
        "last_event_time"
    )
)

# ============================================================
# Write to PostgreSQL FACT table
# ============================================================
(
    df_final
    .write
    .mode("append")
    .jdbc(
        url=jdbc_url,
        table="analytics.fact_inventory_balance",
        properties=jdbc_props
    )
)

spark.stop()

