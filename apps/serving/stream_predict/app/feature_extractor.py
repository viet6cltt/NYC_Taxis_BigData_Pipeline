"""
Streaming feature engineering for started trips.

The serving path does not persist an intermediate feature table. It reads
silver/trip_started, joins the static route_estimates lookup, builds the
training-compatible FEATURE_COLS in memory, then immediately predicts.
"""

import math

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from app.config import N_LOCATION_CLUSTERS, N_TEMPORAL_CLUSTERS


def add_temporal_features(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("pickup_hour", F.hour(F.col("pickup_datetime")))
        .withColumn("pickup_day_of_week", (F.dayofweek(F.col("pickup_datetime")) + 5) % 7)
        .withColumn("is_weekend", F.when(F.col("pickup_day_of_week").isin(5, 6), 1).otherwise(0))
        .withColumn("year_month", F.date_format(F.col("pickup_datetime"), "yyyy-MM"))
    )


def add_cyclical_features(df: DataFrame) -> DataFrame:
    two_pi = 2.0 * math.pi
    return (
        df
        .withColumn("hour_sin", F.sin(F.col("pickup_hour") * F.lit(two_pi / 24.0)))
        .withColumn("hour_cos", F.cos(F.col("pickup_hour") * F.lit(two_pi / 24.0)))
        .withColumn("day_sin", F.sin(F.col("pickup_day_of_week") * F.lit(two_pi / 7.0)))
        .withColumn("day_cos", F.cos(F.col("pickup_day_of_week") * F.lit(two_pi / 7.0)))
    )


def enrich_with_route_estimates(df: DataFrame, route_estimates_df: DataFrame) -> DataFrame:
    exact = (
        route_estimates_df
        .filter(F.col("estimate_level") == "route_time")
        .select(
            F.col("pulocation_id").alias("exact_pulocation_id"),
            F.col("dolocation_id").alias("exact_dolocation_id"),
            F.col("pickup_hour").alias("exact_pickup_hour"),
            F.col("pickup_day_of_week").alias("exact_pickup_day_of_week"),
            F.col("estimated_trip_distance").alias("exact_estimated_trip_distance"),
            F.col("estimated_trip_duration_seconds").alias("exact_estimated_trip_duration_seconds"),
            F.col("sample_count").alias("exact_sample_count"),
        )
    )
    route = (
        route_estimates_df
        .filter(F.col("estimate_level") == "route")
        .select(
            F.col("pulocation_id").alias("route_pulocation_id"),
            F.col("dolocation_id").alias("route_dolocation_id"),
            F.col("estimated_trip_distance").alias("route_estimated_trip_distance"),
            F.col("estimated_trip_duration_seconds").alias("route_estimated_trip_duration_seconds"),
            F.col("sample_count").alias("route_sample_count_raw"),
        )
    )
    global_estimate = (
        route_estimates_df
        .filter(F.col("estimate_level") == "global")
        .select(
            F.lit(1).alias("_global_join_key"),
            F.col("estimated_trip_distance").alias("global_estimated_trip_distance"),
            F.col("estimated_trip_duration_seconds").alias("global_estimated_trip_duration_seconds"),
            F.col("sample_count").alias("global_sample_count"),
        )
        .limit(1)
    )

    return (
        df
        .join(
            F.broadcast(exact),
            (
                (F.col("pulocation_id") == F.col("exact_pulocation_id"))
                & (F.col("dolocation_id") == F.col("exact_dolocation_id"))
                & (F.col("pickup_hour") == F.col("exact_pickup_hour"))
                & (F.col("pickup_day_of_week") == F.col("exact_pickup_day_of_week"))
            ),
            "left",
        )
        .join(
            F.broadcast(route),
            (
                (F.col("pulocation_id") == F.col("route_pulocation_id"))
                & (F.col("dolocation_id") == F.col("route_dolocation_id"))
            ),
            "left",
        )
        .withColumn("_global_join_key", F.lit(1))
        .join(F.broadcast(global_estimate), "_global_join_key", "left")
        .drop(
            "_global_join_key",
            "exact_pulocation_id",
            "exact_dolocation_id",
            "exact_pickup_hour",
            "exact_pickup_day_of_week",
            "route_pulocation_id",
            "route_dolocation_id",
        )
        .withColumn(
            "estimated_trip_distance",
            F.coalesce(
                F.col("exact_estimated_trip_distance"),
                F.col("route_estimated_trip_distance"),
                F.col("global_estimated_trip_distance"),
            ),
        )
        .withColumn(
            "estimated_trip_duration_seconds",
            F.coalesce(
                F.col("exact_estimated_trip_duration_seconds"),
                F.col("route_estimated_trip_duration_seconds"),
                F.col("global_estimated_trip_duration_seconds"),
            ),
        )
        .withColumn(
            "estimate_level",
            F.when(F.col("exact_sample_count").isNotNull(), F.lit("route_time"))
            .when(F.col("route_sample_count_raw").isNotNull(), F.lit("route"))
            .otherwise(F.lit("global")),
        )
        .withColumn(
            "route_sample_count",
            F.coalesce(F.col("exact_sample_count"), F.col("route_sample_count_raw"), F.col("global_sample_count")),
        )
        .drop(
            "exact_estimated_trip_distance",
            "exact_estimated_trip_duration_seconds",
            "exact_sample_count",
            "route_estimated_trip_distance",
            "route_estimated_trip_duration_seconds",
            "route_sample_count_raw",
            "global_estimated_trip_distance",
            "global_estimated_trip_duration_seconds",
            "global_sample_count",
        )
    )


def add_distance_and_cluster_features(df: DataFrame) -> DataFrame:
    duration_hours = F.col("estimated_trip_duration_seconds") / F.lit(3600.0)
    temporal_bucket_size = max(1, 24 // N_TEMPORAL_CLUSTERS)
    return (
        df
        .withColumn(
            "estimated_speed",
            F.when(duration_hours > 0, F.col("estimated_trip_distance") / duration_hours).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "distance_manhattan",
            F.abs(F.col("dolocation_id").cast("double") - F.col("pulocation_id").cast("double")),
        )
        .withColumn(
            "location_cluster",
            ((F.col("pulocation_id") + F.col("dolocation_id")) % F.lit(N_LOCATION_CLUSTERS)).cast("int"),
        )
        .withColumn(
            "temporal_cluster",
            F.floor(F.col("pickup_hour") / F.lit(temporal_bucket_size)).cast("int"),
        )
    )


def extract_features(started_df: DataFrame, route_estimates_df: DataFrame) -> DataFrame:
    df = (
        started_df
        .filter(F.col("trip_id").isNotNull())
        .filter(F.col("pickup_datetime").isNotNull())
        .filter(F.col("pulocation_id").isNotNull())
        .filter(F.col("dolocation_id").isNotNull())
    )
    if "event_id" in df.columns:
        df = df.withColumnRenamed("event_id", "started_event_id")

    df = add_temporal_features(df)
    df = enrich_with_route_estimates(df, route_estimates_df)
    df = add_cyclical_features(df)
    df = add_distance_and_cluster_features(df)
    return df.filter(
        F.col("estimated_trip_distance").isNotNull()
        & (F.col("estimated_trip_distance") > 0)
        & F.col("estimated_trip_duration_seconds").isNotNull()
        & (F.col("estimated_trip_duration_seconds") > 0)
    )
