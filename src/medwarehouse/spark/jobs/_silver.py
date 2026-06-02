"""Shared Silver deduplication and quarantine logic for all Kafka domains."""
from __future__ import annotations

from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window


def split_silver_quarantine(
    parsed_df: DataFrame,
    *,
    validation_builder: Callable[[DataFrame], DataFrame],
) -> tuple[DataFrame, DataFrame]:
    """
    Apply validation, deduplication, and quarantine splitting to a parsed Bronze DataFrame.

    Args:
        parsed_df: DataFrame already parsed from raw JSON with event_id, event_time_ts,
                   dedupe_key, and all domain-specific payload columns.
        validation_builder: Callable that adds `validation_errors_raw` and
                            `validation_errors` columns to the DataFrame.

    Returns:
        (silver_df, quarantine_df) — mutually exclusive partitions.
    """
    validated = validation_builder(parsed_df)

    window = Window.partitionBy("dedupe_key").orderBy(
        F.col("source_kafka_timestamp").desc_nulls_last(),
        F.col("source_offset").desc_nulls_last(),
    )
    ranked = validated.withColumn("dedupe_rank", F.row_number().over(window)).withColumn(
        "duplicate_error",
        F.when(F.col("dedupe_rank") > 1, F.lit("duplicate_event_id")),
    )

    quarantined = (
        ranked.withColumn(
            "quarantine_reasons_raw",
            F.concat(F.col("validation_errors"), F.array(F.col("duplicate_error"))),
        )
        .withColumn(
            "quarantine_reasons",
            F.expr("filter(quarantine_reasons_raw, x -> x is not null)"),
        )
        .drop("validation_errors_raw", "quarantine_reasons_raw", "duplicate_error")
    )

    silver = (
        quarantined.filter(F.size(F.col("quarantine_reasons")) == 0)
        .drop("validation_errors", "quarantine_reasons", "dedupe_key", "dedupe_rank")
        .withColumnRenamed("event_time_ts", "event_time")
    )
    quarantine = (
        quarantined.filter(F.size(F.col("quarantine_reasons")) > 0)
        .drop("validation_errors_raw", "dedupe_key")
        .withColumnRenamed("event_time_ts", "event_time")
    )
    return silver, quarantine


def build_validation_array(*conditions) -> DataFrame:
    """Helper: build a validation_errors column from a list of F.when expressions."""
    return F.array(*conditions)
