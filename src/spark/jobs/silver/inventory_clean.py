from pyspark.sql.functions import col

valid_df = silver_df.filter(
    col("product_id").isNotNull() &
    col("warehouse_id").isNotNull() &
    col("quantity_delta").isNotNull()
)

invalid_df = silver_df.subtract(valid_df)

invalid_df.write.mode("append").parquet(
    "data/quarantine/inventory_events"
)

valid_df.write.mode("overwrite").parquet(
    "data/silver/inventory_events"
)

