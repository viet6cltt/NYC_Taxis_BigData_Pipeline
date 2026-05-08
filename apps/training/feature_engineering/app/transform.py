"""
Feature Engineering: Silver → Gold
Replicates the Kaggle notebook logic in PySpark.

Features created:
  Temporal  : pickup_hour, pickup_day_of_week, is_weekend
  Cyclical  : hour_sin, hour_cos, day_sin, day_cos
  Speed     : speed (miles / hour)
  Distance  : distance_manhattan (abs diff of location IDs as proxy)
  Clusters  : location_cluster, temporal_cluster (KMeans via pandas UDF)
"""

import math
import numpy as np
import pandas as pd

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, hour, dayofweek, sin, cos, lit, unix_timestamp, when, abs as spark_abs,
    pandas_udf
)
from pyspark.sql.types import IntegerType

from app.config import N_LOCATION_CLUSTERS, N_TEMPORAL_CLUSTERS


# ---------------------------------------------------------------------------
# Temporal features
# ---------------------------------------------------------------------------

def add_temporal_features(df: DataFrame) -> DataFrame:
    """Extract hour-of-day and day-of-week, plus is_weekend flag."""
    df = (
        df
        .withColumn("pickup_hour",        hour(col("pickup_datetime")))
        # Spark dayofweek: 1=Sun … 7=Sat  →  shift to 0=Mon … 6=Sun
        .withColumn("pickup_day_of_week", (dayofweek(col("pickup_datetime")) + 5) % 7)
        .withColumn("is_weekend",
                    when(col("pickup_day_of_week").isin(5, 6), 1).otherwise(0))
    )
    return df


# ---------------------------------------------------------------------------
# Cyclical encoding  (hour, day)
# ---------------------------------------------------------------------------

def add_cyclical_features(df: DataFrame) -> DataFrame:
    """Sine/cosine encoding so hour 0 ≈ hour 23 in feature space."""
    two_pi = 2.0 * math.pi
    df = (
        df
        .withColumn("hour_sin", sin(col("pickup_hour")        * lit(two_pi / 24.0)))
        .withColumn("hour_cos", cos(col("pickup_hour")        * lit(two_pi / 24.0)))
        .withColumn("day_sin",  sin(col("pickup_day_of_week") * lit(two_pi / 7.0)))
        .withColumn("day_cos",  cos(col("pickup_day_of_week") * lit(two_pi / 7.0)))
    )
    return df


# ---------------------------------------------------------------------------
# Distance / speed
# ---------------------------------------------------------------------------

def add_distance_speed(df: DataFrame) -> DataFrame:
    """Manhattan distance proxy from location IDs, and average speed."""
    df = df.withColumn(
        "distance_manhattan",
        spark_abs(col("dolocation_id").cast("double") - col("pulocation_id").cast("double"))
    )

    # trip_duration_seconds already exists in Silver (from bronze_to_silver/transform.py)
    # speed in miles per hour
    trip_hours = col("trip_duration_seconds") / lit(3600.0)
    df = df.withColumn(
        "speed",
        when(trip_hours > 0, col("trip_distance") / trip_hours).otherwise(lit(0.0))
    )
    return df


# ---------------------------------------------------------------------------
# KMeans clustering  (pandas UDF applied per partition)
# ---------------------------------------------------------------------------

def add_location_cluster(df: DataFrame, n_clusters: int = N_LOCATION_CLUSTERS) -> DataFrame:
    """
    Assign a location cluster label using mini-batch KMeans on
    (pulocation_id, dolocation_id).
    Runs via mapInPandas to keep Spark happy (returns iterator of DataFrames).
    """
    from sklearn.cluster import MiniBatchKMeans
    from sklearn.preprocessing import StandardScaler

    # Collect sample for fitting (cheap — location IDs are int)
    sample_pd = (
        df.select("pulocation_id", "dolocation_id")
          .dropna()
          .limit(50_000)
          .toPandas()
    )

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(sample_pd.values.astype(float))
    kmeans = MiniBatchKMeans(n_clusters=n_clusters, random_state=42,
                             batch_size=1024, n_init=3)
    kmeans.fit(X_scaled)

    # Broadcast model artifacts to workers
    sc = df.sparkSession.sparkContext
    bc_kmeans  = sc.broadcast(kmeans)
    bc_scaler  = sc.broadcast(scaler)
    feature_names = ["pulocation_id", "dolocation_id"]

    def _predict(iterator):
        km  = bc_kmeans.value
        sc_ = bc_scaler.value
        for pdf in iterator:
            X = pdf[feature_names].fillna(0).values.astype(float)
            pdf["location_cluster"] = km.predict(sc_.transform(X)).astype(int)
            yield pdf

    from pyspark.sql.types import StructType, StructField, IntegerType

    out_schema = StructType(df.schema.fields + [
        StructField("location_cluster", IntegerType(), True)
    ])

    return df.mapInPandas(_predict, schema=out_schema)


def add_temporal_cluster(df: DataFrame, n_clusters: int = N_TEMPORAL_CLUSTERS) -> DataFrame:
    """
    Assign a temporal cluster label using mini-batch KMeans on
    (pickup_hour, pickup_day_of_week).
    """
    from sklearn.cluster import MiniBatchKMeans
    from sklearn.preprocessing import StandardScaler

    sample_pd = (
        df.select("pickup_hour", "pickup_day_of_week")
          .dropna()
          .limit(50_000)
          .toPandas()
    )

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(sample_pd.values.astype(float))
    kmeans = MiniBatchKMeans(n_clusters=n_clusters, random_state=42,
                             batch_size=1024, n_init=3)
    kmeans.fit(X_scaled)

    sc = df.sparkSession.sparkContext
    bc_kmeans  = sc.broadcast(kmeans)
    bc_scaler  = sc.broadcast(scaler)
    feature_names = ["pickup_hour", "pickup_day_of_week"]

    def _predict(iterator):
        km  = bc_kmeans.value
        sc_ = bc_scaler.value
        for pdf in iterator:
            X = pdf[feature_names].fillna(0).values.astype(float)
            pdf["temporal_cluster"] = km.predict(sc_.transform(X)).astype(int)
            yield pdf

    from pyspark.sql.types import StructType, StructField, IntegerType

    out_schema = StructType(df.schema.fields + [
        StructField("temporal_cluster", IntegerType(), True)
    ])
    
    return df.mapInPandas(_predict, schema=out_schema)


# ---------------------------------------------------------------------------
# Final column selection
# ---------------------------------------------------------------------------

GOLD_FEATURE_COLS = [
    "trip_id",
    # Target
    "fare_amount",
    # Raw trip fields
    "passenger_count",
    "trip_distance",
    "trip_duration_seconds",
    "pulocation_id",
    "dolocation_id",
    # Engineered
    "pickup_hour",
    "pickup_day_of_week",
    "is_weekend",
    "hour_sin",
    "hour_cos",
    "day_sin",
    "day_cos",
    "distance_manhattan",
    "speed",
    "location_cluster",
    "temporal_cluster",
    # Partitioning
    "year_month",
]


def transform(silver_df: DataFrame) -> DataFrame:
    """Full feature engineering pipeline Silver → Gold."""
    df = silver_df
    
    # HOTFIX: recreate column if missing in Silver, we will delete it after run again bronze_to_silver with the new logic
    if "trip_duration_seconds" not in df.columns:
        df = df.withColumn(
            "trip_duration_seconds",
            unix_timestamp(col("dropoff_datetime")) -
            unix_timestamp(col("pickup_datetime"))
        )

    # Drop rows without target or key fields
    df = df.filter(col("fare_amount").isNotNull() & (col("fare_amount") > 0))
    df = df.filter(col("trip_duration_seconds").isNotNull() & (col("trip_duration_seconds") > 0))
    df = df.filter(col("trip_distance").isNotNull() & (col("trip_distance") > 0))

    df = add_temporal_features(df)
    df = add_cyclical_features(df)
    df = add_distance_speed(df)
    df = add_location_cluster(df)
    df = add_temporal_cluster(df)

    gold_df = df.select(*GOLD_FEATURE_COLS)
    print(f"[feature_engineering] Gold schema ready. Columns: {gold_df.columns}")
    return gold_df
