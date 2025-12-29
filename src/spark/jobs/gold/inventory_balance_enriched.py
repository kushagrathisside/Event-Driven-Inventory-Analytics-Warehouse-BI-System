from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ============================================================
# Spark Session
# ============================================================
spark = (
    SparkSession.builder
    .appName("InventoryGoldBalanceEnriched")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# Paths
# ============================================================
GOLD_BASE_PATH = "data/gold/inventory_balance"
GOLD_ENRICHED_PATH = "data/gold/inventory_balance_enriched"

# ============================================================
# PostgreSQL JDBC Configuration
# ============================================================
jdbc_url = "jdbc:postgresql://localhost:5432/medwarehouse_master"

jdbc_props = {
    "user": "spark_reader",
    "password": "spark_reader_pwd",
    "driver": "org.postgresql.Driver"
}

# ============================================================
# Read Gold Inventory Balance (FACT)
# ============================================================
inventory_balance = spark.read.parquet(GOLD_BASE_PATH)

# ============================================================
# Read Master Tables (DIMENSIONS)
# ============================================================
products = (
    spark.read
    .jdbc(jdbc_url, "products", properties=jdbc_props)
    .select(
        col("product_id").alias("p_product_id"),
        col("supplier_id"),
        col("brand_name"),
        col("generic_name"),
        col("form_factor"),
        col("hsn_code")
    )
)

suppliers = (
    spark.read
    .jdbc(jdbc_url, "suppliers", properties=jdbc_props)
    .select(
        col("supplier_id").alias("s_supplier_id"),
        col("name").alias("supplier_name"),
        col("gstin")
    )
)

warehouses = (
    spark.read
    .jdbc(jdbc_url, "warehouse_locations", properties=jdbc_props)
    .select(
        col("location_id").alias("w_location_id"),
        col("name").alias("warehouse_name"),
        col("temperature_range")
    )
)

# ============================================================
# Enrichment (SCHEMA-CORRECT)
# ============================================================
inventory_enriched = (
    inventory_balance
    # inventory → products
    .join(
        products,
        inventory_balance.product_id == products.p_product_id,
        how="left"
    )
    # products → suppliers
    .join(
        suppliers,
        products.supplier_id == suppliers.s_supplier_id,
        how="left"
    )
    # inventory → warehouse
    .join(
        warehouses,
        inventory_balance.warehouse_id == warehouses.w_location_id,
        how="left"
    )
    # final projection
    .select(
        inventory_balance.product_id,
        col("brand_name"),
        col("generic_name"),
        col("form_factor"),
        col("hsn_code"),

        col("supplier_name"),
        col("gstin"),

        inventory_balance.warehouse_id,
        col("warehouse_name"),
        col("temperature_range"),

        inventory_balance.batch_number,
        inventory_balance.expiry_date,
        inventory_balance.current_quantity,
        inventory_balance.last_event_time
    )
)

# ============================================================
# Write Gold Enriched Layer
# ============================================================
(
    inventory_enriched
    .write
    .mode("overwrite")
    .parquet(GOLD_ENRICHED_PATH)
)

print("✅ Inventory Gold Enriched layer written successfully")

