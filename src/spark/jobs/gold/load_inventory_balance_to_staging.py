from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# ============================================================
# Spark Session
# ============================================================
spark = (
    SparkSession.builder
    .appName("LoadInventoryBalanceToStaging")
    .getOrCreate()
)

# ============================================================
# Paths
# ============================================================
GOLD_PATH = "data/gold/inventory_balance"

# ============================================================
# JDBC Config
# ============================================================
jdbc_url = "jdbc:postgresql://localhost:5432/medwarehouse_analytics"

jdbc_props = {
    "user": "spark_writer",
    "password": "spark_writer_pwd",
    "driver": "org.postgresql.Driver"
}

# ============================================================
# Read Gold Inventory Balance
# ============================================================
df = spark.read.parquet(GOLD_PATH)

# ============================================================
# Basic safety filter
# ============================================================
df_clean = (
    df
    .filter(col("product_id").isNotNull())
    .filter(col("warehouse_id").isNotNull())
    .filter(col("batch_number").isNotNull())
    .withColumn("expiry_date", to_date(col("expiry_date")))
)

df_clean.printSchema()

# ============================================================
# Write to STAGING table (NO CONSTRAINTS)
# ============================================================
(
    df_clean
    .write
    .mode("append")
    .jdbc(
        url=jdbc_url,
        table="analytics.fact_inventory_balance_stg",
        properties=jdbc_props
    )
)

spark.stop()

