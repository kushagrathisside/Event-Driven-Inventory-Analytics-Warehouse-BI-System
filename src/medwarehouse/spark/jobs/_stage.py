"""Shared JDBC staging write for all domains."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from medwarehouse.config import PostgresConnection
from medwarehouse.logging import get_logger


logger = get_logger(__name__)


def write_to_staging(
    spark: SparkSession,
    df: DataFrame,
    *,
    connection: PostgresConnection,
    table: str,
    domain_label: str,
) -> int:
    """Truncate-and-load a Silver DataFrame into an analytics staging table via JDBC."""
    row_count = df.count()
    logger.info(
        "Writing %s staging rows=%s jdbc=%s table=%s",
        domain_label, row_count, connection.jdbc_url, table,
    )
    (
        df.write.format("jdbc")
        .option("url", connection.jdbc_url)
        .option("dbtable", table)
        .option("user", connection.user)
        .option("password", connection.password)
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )
    spark.stop()
    return row_count
