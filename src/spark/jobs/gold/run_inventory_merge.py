from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("RunInventoryMerge")
    .getOrCreate()
)

jdbc_url = "jdbc:postgresql://localhost:5432/medwarehouse_analytics"
props = {
    "user": "spark_writer",
    "password": "spark_writer_pwd",
    "driver": "org.postgresql.Driver"
}

spark.read.jdbc(
    url=jdbc_url,
    table="(SELECT analytics.merge_inventory_balance()) as t",
    properties=props
).count()

spark.stop()

