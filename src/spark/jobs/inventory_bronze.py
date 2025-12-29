from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

spark = SparkSession.builder \
    .appName("InventoryBronzeIngestion") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "inventory_events") \
    .option("startingOffsets", "earliest") \
    .load()

raw = df.select(
    col("key").cast(StringType()).alias("kafka_key"),
    col("value").cast(StringType()).alias("raw_event"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp")
)

query = raw.writeStream \
    .format("parquet") \
    .option("path", "data/bronze/inventory_events") \
    .option("checkpointLocation", "data/checkpoints/inventory_events") \
    .outputMode("append") \
    .start()

query.awaitTermination()

