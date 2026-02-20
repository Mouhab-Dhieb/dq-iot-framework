from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window


def apply_row_level_checks(events: DataFrame, rules: DataFrame) -> DataFrame:
    """
    Apply config-driven data quality checks.
    Expects events columns: sensor_id, tag, event_time, ingest_time, value
    Returns events enriched with:
      - value_double
      - is_valid
      - failure_reason (array<string>)
      - is_duplicate, out_of_range, too_late, cast_failed, null flags
    """
    df = events.join(rules.where(F.col("enabled") == True), on="tag", how="left")

    # Null checks
    df = df.withColumn("is_null_sensor", F.col("sensor_id").isNull())
    df = df.withColumn("is_null_event_time", F.col("event_time").isNull())
    df = df.withColumn("is_null_value", F.col("value").isNull())

    # Type/cast checks (focus numeric types in this demo)
    df = df.withColumn("value_double", F.col("value").cast("double"))
    df = df.withColumn("cast_failed", F.col("value").isNotNull() & F.col("value_double").isNull())

    # Range checks
    df = df.withColumn(
        "out_of_range",
        (F.col("value_double").isNotNull())
        & (
            (F.col("min_value").isNotNull() & (F.col("value_double") < F.col("min_value")))
            | (F.col("max_value").isNotNull() & (F.col("value_double") > F.col("max_value")))
        ),
    )

    # Lateness
    df = df.withColumn(
        "lateness_sec",
        F.unix_timestamp("ingest_time") - F.unix_timestamp("event_time"),
    )
    df = df.withColumn(
        "too_late",
        F.col("max_lateness_sec").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("ingest_time").isNotNull()
        & (F.col("lateness_sec") > F.col("max_lateness_sec")),
    )

    # Dedup: same (sensor_id, tag, event_time) appearing more than once
    w = Window.partitionBy("sensor_id", "tag", "event_time").orderBy(
        F.col("ingest_time").asc_nulls_last()
    )
    df = df.withColumn("dup_rank", F.row_number().over(w))
    df = df.withColumn("is_duplicate", F.col("dup_rank") > 1)

    failure_reason = F.array_remove(
        F.array(
            F.when(F.col("rule_id").isNull(), F.lit("NO_RULE_FOUND")),
            F.when(F.col("is_null_sensor"), F.lit("NULL_SENSOR_ID")),
            F.when(F.col("is_null_event_time"), F.lit("NULL_EVENT_TIME")),
            F.when(F.col("is_null_value"), F.lit("NULL_VALUE")),
            F.when(F.col("cast_failed"), F.lit("CAST_FAILED")),
            F.when(F.col("out_of_range"), F.lit("OUT_OF_RANGE")),
            F.when(F.col("is_duplicate"), F.lit("DUPLICATE")),
            F.when(F.col("too_late"), F.lit("TOO_LATE")),
        ),
        F.lit(None),
    )

    df = df.withColumn("failure_reason", failure_reason)
    df = df.withColumn("is_valid", F.size("failure_reason") == 0)

    return df