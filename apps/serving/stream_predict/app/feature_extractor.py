"""
Stateless feature engineering applied on each Spark Streaming micro-batch.
Mirrors apps/training/feature_engineering/app/transform.py but:
  - Works on the decoded Kafka event schema (Silver-like fields)
  - Uses fixed KMeans cluster assignments via simple location zone binning
    (no model fit needed — keeps stateless)
"""

import math
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, hour, dayofweek, sin, cos, lit, when, abs as spark_abs,
)


def add_temporal_features(df: DataFrame) -> DataFrame:
    df = (
        df
        .withColumn("pickup_hour",        hour(col("pickup_datetime")))
        .withColumn("pickup_day_of_week", (dayofweek(col("pickup_datetime")) + 5) % 7)
        .withColumn("is_weekend",
                    when(col("pickup_day_of_week").isin(5, 6), 1).otherwise(0))
    )
    return df


def add_cyclical_features(df: DataFrame) -> DataFrame:
    two_pi = 2.0 * math.pi
    df = (
        df
        .withColumn("hour_sin", sin(col("pickup_hour")        * lit(two_pi / 24.0)))
        .withColumn("hour_cos", cos(col("pickup_hour")        * lit(two_pi / 24.0)))
        .withColumn("day_sin",  sin(col("pickup_day_of_week") * lit(two_pi / 7.0)))
        .withColumn("day_cos",  cos(col("pickup_day_of_week") * lit(two_pi / 7.0)))
    )
    return df


def add_distance_speed(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "distance_manhattan",
        spark_abs(col("dolocation_id").cast("double") - col("pulocation_id").cast("double"))
    )
    trip_hours = col("trip_duration_seconds") / lit(3600.0)
    df = df.withColumn(
        "speed",
        when(trip_hours > 0, col("trip_distance") / trip_hours).otherwise(lit(0.0))
    )
    return df


def add_cluster_proxies(df: DataFrame) -> DataFrame:
    """
    Lightweight, stateless cluster proxy using modulo bucketing.
    Avoids fitting KMeans at inference time.
    """
    df = df.withColumn(
        "location_cluster",
        ((col("pulocation_id") + col("dolocation_id")) % lit(5)).cast("int")
    )
    df = df.withColumn(
        "temporal_cluster",
        ((col("pickup_hour") // lit(6))).cast("int")   # 0-5h, 6-11h, 12-17h, 18-23h → 4 clusters
    )
    return df


def extract_features(df: DataFrame) -> DataFrame:
    """Full stateless feature extraction for streaming inference."""
    # trip_duration_seconds may already exist from Avro payload; compute if not
    from pyspark.sql.functions import unix_timestamp
    if "trip_duration_seconds" not in df.columns:
        df = df.withColumn(
            "trip_duration_seconds",
            unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))
        )

    df = add_temporal_features(df)
    df = add_cyclical_features(df)
    df = add_distance_speed(df)
    df = add_cluster_proxies(df)
    return df
