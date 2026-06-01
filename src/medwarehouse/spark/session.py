from __future__ import annotations

from pyspark.sql import SparkSession

from medwarehouse.config import get_settings


def build_spark_session(app_name: str) -> SparkSession:
    settings = get_settings()
    builder = (
        SparkSession.builder.appName(app_name)
        .master(settings.spark.master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )

    if settings.spark.postgres_driver_jar.exists():
        builder = builder.config("spark.jars", str(settings.spark.postgres_driver_jar))

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(settings.spark.log_level)
    return spark
