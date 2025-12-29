from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("GoldInventoryToPostgres")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# Read Gold Parquet
# -----------------------------
gold_df = spark.read.parquet(
    "data/gold/inventory_balance"
)

# -----------------------------
# Write to Postgres
# -----------------------------
(
    gold_df.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/medwarehouse")
    .option("dbtable", "inventory_balance")
    .option("user", "meduser")
    .option("password", "medpass")
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")   # SAFE for now
    .save()
)

spark.stop()

