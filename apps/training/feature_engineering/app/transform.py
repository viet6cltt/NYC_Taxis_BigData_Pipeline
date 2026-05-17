"""
Gold ML batch transforms.

This app intentionally keeps the Gold layer small:
  - route_estimates: offline lookup for started-trip serving
  - features: training table with the same feature contract as serving
  - prediction_actuals: delayed-label evaluation table
  - model_quality_daily: daily model metrics
"""

import math

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from app.config import (
    MAX_AVG_SPEED_MPH,
    MAX_FARE_AMOUNT,
    MAX_PASSENGER_COUNT,
    MAX_TOTAL_AMOUNT,
    MAX_TRIP_DISTANCE,
    MAX_TRIP_DURATION_SECONDS,
    MIN_AVG_SPEED_MPH,
    MIN_FARE_AMOUNT,
    MIN_TRIP_DISTANCE,
    MIN_TRIP_DURATION_SECONDS,
    N_LOCATION_CLUSTERS,
    N_TEMPORAL_CLUSTERS,
)


FEATURE_COLS = [
    "passenger_count",
    "estimated_trip_distance",
    "estimated_trip_duration_seconds",
    "estimated_speed",
    "pickup_hour",
    "pickup_day_of_week",
    "is_weekend",
    "hour_sin",
    "hour_cos",
    "day_sin",
    "day_cos",
    "distance_manhattan",
    "location_cluster",
    "temporal_cluster",
]

TRAINING_FEATURE_COLUMNS = [
    "trip_id",
    "fare_amount",
    *FEATURE_COLS,
    "pulocation_id",
    "dolocation_id",
    "actual_trip_distance",
    "actual_trip_duration_seconds",
    "estimate_level",
    "route_sample_count",
    "year_month",
]


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


def _valid_completed_trips(df: DataFrame) -> DataFrame:
    duration_hours = F.col("trip_duration_seconds") / F.lit(3600.0)
    avg_speed_mph = F.col("trip_distance") / duration_hours
    return (
        df
        .filter(F.col("trip_id").isNotNull())
        .filter(F.col("pickup_datetime").isNotNull())
        .filter(F.col("dropoff_datetime").isNotNull())
        .filter(F.col("pulocation_id").isNotNull())
        .filter(F.col("dolocation_id").isNotNull())
        .filter(F.col("passenger_count").isNotNull())
        .filter((F.col("passenger_count") > 0) & (F.col("passenger_count") <= MAX_PASSENGER_COUNT))
        .filter(F.col("trip_distance").isNotNull())
        .filter(F.col("trip_distance").between(MIN_TRIP_DISTANCE, MAX_TRIP_DISTANCE))
        .filter(F.col("trip_duration_seconds").isNotNull())
        .filter(F.col("trip_duration_seconds").between(MIN_TRIP_DURATION_SECONDS, MAX_TRIP_DURATION_SECONDS))
        .filter(avg_speed_mph.between(MIN_AVG_SPEED_MPH, MAX_AVG_SPEED_MPH))
        .filter(F.col("fare_amount").isNotNull())
        .filter(F.col("fare_amount").between(MIN_FARE_AMOUNT, MAX_FARE_AMOUNT))
        .filter(F.col("total_amount").isNotNull())
        .filter((F.col("total_amount") >= 0) & (F.col("total_amount") <= MAX_TOTAL_AMOUNT))
    )


def _estimate_agg(df: DataFrame, group_cols: list[str], estimate_level: str) -> DataFrame:
    selected_group_cols = [F.col(name) for name in group_cols]
    agg_df = (
        df
        .groupBy(*selected_group_cols)
        .agg(
            F.count(F.lit(1)).cast("long").alias("sample_count"),
            F.avg("trip_distance").cast("double").alias("avg_trip_distance"),
            F.expr("percentile_approx(trip_distance, 0.5)").cast("double").alias("median_trip_distance"),
            F.avg("trip_duration_seconds").cast("double").alias("avg_trip_duration_seconds"),
            F.expr("percentile_approx(trip_duration_seconds, 0.5)").cast("double").alias("median_trip_duration_seconds"),
        )
        .withColumn("estimate_level", F.lit(estimate_level))
    )

    for name, dtype in [
        ("pulocation_id", "int"),
        ("dolocation_id", "int"),
        ("pickup_hour", "int"),
        ("pickup_day_of_week", "int"),
    ]:
        if name not in group_cols:
            agg_df = agg_df.withColumn(name, F.lit(None).cast(dtype))

    return agg_df.select(
        "estimate_level",
        "pulocation_id",
        "dolocation_id",
        "pickup_hour",
        "pickup_day_of_week",
        "sample_count",
        "avg_trip_distance",
        "median_trip_distance",
        "avg_trip_duration_seconds",
        "median_trip_duration_seconds",
    )


def build_route_estimates(completed_df: DataFrame) -> DataFrame:
    completed = add_temporal_features(_valid_completed_trips(completed_df))
    route_time = _estimate_agg(
        completed,
        ["pulocation_id", "dolocation_id", "pickup_hour", "pickup_day_of_week"],
        "route_time",
    )
    route_only = _estimate_agg(
        completed,
        ["pulocation_id", "dolocation_id"],
        "route",
    )
    global_estimate = _estimate_agg(completed, [], "global")

    route_estimates = (
        route_time
        .unionByName(route_only)
        .unionByName(global_estimate)
        .withColumn("estimated_trip_distance", F.col("median_trip_distance").cast("double"))
        .withColumn("estimated_trip_duration_seconds", F.col("median_trip_duration_seconds").cast("double"))
        .withColumn("updated_at", F.current_timestamp())
    )
    return route_estimates


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

    enriched = (
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
    return enriched


def build_training_features(completed_df: DataFrame, route_estimates_df: DataFrame) -> DataFrame:
    completed = (
        add_temporal_features(_valid_completed_trips(completed_df))
        .withColumnRenamed("trip_distance", "actual_trip_distance")
        .withColumnRenamed("trip_duration_seconds", "actual_trip_duration_seconds")
    )

    # Route lookup expects these canonical actual columns for the lookup source only.
    lookup_input = (
        completed
        .withColumn("trip_distance", F.col("actual_trip_distance"))
        .withColumn("trip_duration_seconds", F.col("actual_trip_duration_seconds"))
    )
    df = enrich_with_route_estimates(lookup_input, route_estimates_df)
    df = add_cyclical_features(df)
    df = add_distance_and_cluster_features(df)
    df = df.filter(
        F.col("estimated_trip_distance").isNotNull()
        & F.col("estimated_trip_distance").between(MIN_TRIP_DISTANCE, MAX_TRIP_DISTANCE)
        & F.col("estimated_trip_duration_seconds").isNotNull()
        & F.col("estimated_trip_duration_seconds").between(MIN_TRIP_DURATION_SECONDS, MAX_TRIP_DURATION_SECONDS)
        & F.col("estimated_speed").between(MIN_AVG_SPEED_MPH, MAX_AVG_SPEED_MPH)
    )
    return df.select(*TRAINING_FEATURE_COLUMNS)


def build_prediction_actuals(predictions_df: DataFrame, completed_df: DataFrame) -> DataFrame:
    prediction_window = Window.partitionBy("trip_id", "model_name", "model_version").orderBy(
        F.col("prediction_timestamp").desc_nulls_last()
    )
    predictions = (
        predictions_df
        .filter(F.col("trip_id").isNotNull())
        .filter(F.col("predicted_fare_amount").isNotNull())
        .withColumn("_rn", F.row_number().over(prediction_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    completed = (
        completed_df
        .filter(F.col("trip_id").isNotNull())
        .filter(F.col("fare_amount").isNotNull())
        .filter(F.col("fare_amount").between(MIN_FARE_AMOUNT, MAX_FARE_AMOUNT))
        .filter(F.col("trip_distance").isNotNull())
        .filter(F.col("trip_distance").between(MIN_TRIP_DISTANCE, MAX_TRIP_DISTANCE))
        .filter(F.col("trip_duration_seconds").isNotNull())
        .filter(F.col("trip_duration_seconds").between(MIN_TRIP_DURATION_SECONDS, MAX_TRIP_DURATION_SECONDS))
        .select(
            F.col("trip_id").alias("completed_trip_id"),
            F.col("event_id").alias("completed_event_id"),
            F.col("ingest_timestamp").alias("actual_arrival_timestamp"),
            F.col("pickup_datetime").alias("actual_pickup_datetime"),
            F.col("dropoff_datetime"),
            F.col("fare_amount").alias("actual_fare_amount"),
            F.col("total_amount").alias("actual_total_amount"),
            F.col("trip_distance").alias("actual_trip_distance"),
            F.col("trip_duration_seconds").alias("actual_trip_duration_seconds"),
            F.col("year_month").alias("actual_year_month"),
        )
    )

    delay_seconds = (
        F.unix_timestamp("actual_arrival_timestamp")
        - F.unix_timestamp("prediction_timestamp")
    )

    joined = (
        predictions
        .join(completed, predictions.trip_id == completed.completed_trip_id, "inner")
        .drop("completed_trip_id")
        .withColumn("prediction_error", F.col("predicted_fare_amount") - F.col("actual_fare_amount"))
        .withColumn("absolute_error", F.abs(F.col("prediction_error")))
        .withColumn("squared_error", F.col("prediction_error") * F.col("prediction_error"))
        .withColumn(
            "label_delay_seconds",
            F.when(
                F.col("prediction_timestamp").isNotNull() & F.col("actual_arrival_timestamp").isNotNull(),
                F.when(delay_seconds >= 0, delay_seconds),
            ),
        )
        .withColumn("year_month", F.coalesce(F.col("year_month"), F.col("actual_year_month")))
        .withColumn("joined_at", F.current_timestamp())
    )

    output_cols = [
        "trip_id",
        "started_event_id",
        "completed_event_id",
        "model_name",
        "model_version",
        "model_stage",
        "prediction_timestamp",
        "actual_arrival_timestamp",
        "label_delay_seconds",
        "predicted_fare_amount",
        "actual_fare_amount",
        "actual_total_amount",
        "prediction_error",
        "absolute_error",
        "squared_error",
        "estimated_trip_distance",
        "estimated_trip_duration_seconds",
        "estimated_speed",
        "actual_trip_distance",
        "actual_trip_duration_seconds",
        "pulocation_id",
        "dolocation_id",
        "estimate_level",
        "route_sample_count",
        "pickup_datetime",
        "actual_pickup_datetime",
        "dropoff_datetime",
        "year_month",
        "joined_at",
    ]
    return joined.select(*[name for name in output_cols if name in joined.columns])


def build_model_quality_daily(prediction_actuals_df: DataFrame) -> DataFrame:
    return (
        prediction_actuals_df
        .filter(F.col("actual_fare_amount").isNotNull())
        .withColumn(
            "metric_date",
            F.to_date(F.coalesce(F.col("dropoff_datetime"), F.col("actual_pickup_datetime"), F.col("actual_arrival_timestamp"))),
        )
        .groupBy("metric_date", "model_name", "model_version")
        .agg(
            F.count(F.lit(1)).cast("long").alias("prediction_count"),
            F.avg("absolute_error").cast("double").alias("mae"),
            F.sqrt(F.avg("squared_error")).cast("double").alias("rmse"),
            F.avg("prediction_error").cast("double").alias("bias"),
            F.avg("predicted_fare_amount").cast("double").alias("avg_predicted_fare"),
            F.avg("actual_fare_amount").cast("double").alias("avg_actual_fare"),
            F.avg("label_delay_seconds").cast("double").alias("avg_label_delay_seconds"),
        )
        .withColumn("updated_at", F.current_timestamp())
    )
