#!/usr/bin/env python3
"""Build a local BI preview lakehouse that matches the production table contract."""

from __future__ import annotations

import math
import shutil
from pathlib import Path

import numpy as np
import pandas as pd
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "_local_delta_store"
TARGET = ROOT / "_local_bi_lakehouse"


def read_delta_parquet_dir(path: Path) -> pd.DataFrame:
    files = sorted(path.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found under {path}")
    return pd.concat([pd.read_parquet(file) for file in files], ignore_index=True)


def build_lifecycle(silver: pd.DataFrame) -> pd.DataFrame:
    df = silver.copy()
    # Bộ sample cũ có trip_id lặp vì nó được tạo trước khi contract lifecycle
    # ra đời. BI preview cần grain "mỗi trip một dòng", nên tạo id ổn định riêng.
    df["trip_id"] = "preview-" + pd.Series(np.arange(len(df)), index=df.index).astype(str)
    df["started_event_id"] = "started-" + df["trip_id"].astype(str)
    df["completed_event_id"] = "completed-" + df["trip_id"].astype(str)
    df["started_event_time"] = df["pickup_datetime"]
    df["completed_event_time"] = df["dropoff_datetime"]
    df["started_ingest_timestamp"] = df["ingest_timestamp"]
    df["completed_ingest_timestamp"] = df["ingest_timestamp"]
    df["status"] = "completed"
    df["year_month"] = pd.to_datetime(df["pickup_datetime"]).dt.strftime("%Y-%m")
    df["created_at"] = df["ingest_timestamp"]
    df["updated_at"] = df["ingest_timestamp"]
    return df[
        [
            "trip_id",
            "status",
            "started_event_id",
            "completed_event_id",
            "started_event_time",
            "completed_event_time",
            "started_ingest_timestamp",
            "completed_ingest_timestamp",
            "trip_date",
            "trip_hour",
            "year_month",
            "vendor_id",
            "pickup_datetime",
            "dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "trip_duration_seconds",
            "rate_code_id",
            "store_and_fwd_flag",
            "pulocation_id",
            "dolocation_id",
            "payment_type",
            "payment_type_desc",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "improvement_surcharge",
            "total_amount",
            "congestion_surcharge",
            "airport_fee",
            "created_at",
            "updated_at",
        ]
    ]


def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    pickup = pd.to_datetime(out["pickup_datetime"])
    out["pickup_hour"] = pickup.dt.hour.astype("int32")
    out["pickup_day_of_week"] = pickup.dt.dayofweek.astype("int32")
    out["is_weekend"] = out["pickup_day_of_week"].isin([5, 6]).astype("int32")
    out["year_month"] = pickup.dt.strftime("%Y-%m")
    return out


def build_route_estimates(lifecycle: pd.DataFrame) -> pd.DataFrame:
    base = add_time_features(lifecycle)

    def agg(cols: list[str], level: str) -> pd.DataFrame:
        if cols:
            out = (
                base.groupby(cols, dropna=False)
                .agg(
                    sample_count=("trip_id", "count"),
                    avg_trip_distance=("trip_distance", "mean"),
                    median_trip_distance=("trip_distance", "median"),
                    avg_trip_duration_seconds=("trip_duration_seconds", "mean"),
                    median_trip_duration_seconds=("trip_duration_seconds", "median"),
                )
                .reset_index()
            )
        else:
            out = pd.DataFrame(
                [
                    {
                        "sample_count": len(base),
                        "avg_trip_distance": base["trip_distance"].mean(),
                        "median_trip_distance": base["trip_distance"].median(),
                        "avg_trip_duration_seconds": base["trip_duration_seconds"].mean(),
                        "median_trip_duration_seconds": base["trip_duration_seconds"].median(),
                    }
                ]
            )
        out["estimate_level"] = level
        for col in ["pulocation_id", "dolocation_id", "pickup_hour", "pickup_day_of_week"]:
            if col not in out:
                out[col] = np.nan
        return out

    result = pd.concat(
        [
            agg(["pulocation_id", "dolocation_id", "pickup_hour", "pickup_day_of_week"], "route_time"),
            agg(["pulocation_id", "dolocation_id"], "route"),
            agg([], "global"),
        ],
        ignore_index=True,
    )
    result["estimated_trip_distance"] = result["median_trip_distance"]
    result["estimated_trip_duration_seconds"] = result["median_trip_duration_seconds"]
    result["updated_at"] = pd.Timestamp.utcnow().tz_localize(None)
    return result[
        [
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
            "estimated_trip_distance",
            "estimated_trip_duration_seconds",
            "updated_at",
        ]
    ]


def build_features(lifecycle: pd.DataFrame) -> pd.DataFrame:
    df = add_time_features(lifecycle)
    df["estimated_trip_distance"] = df["trip_distance"]
    df["estimated_trip_duration_seconds"] = df["trip_duration_seconds"].astype(float)
    df["estimated_speed"] = df["estimated_trip_distance"] / (df["estimated_trip_duration_seconds"] / 3600.0)
    df["hour_sin"] = np.sin(df["pickup_hour"] * 2 * math.pi / 24.0)
    df["hour_cos"] = np.cos(df["pickup_hour"] * 2 * math.pi / 24.0)
    df["day_sin"] = np.sin(df["pickup_day_of_week"] * 2 * math.pi / 7.0)
    df["day_cos"] = np.cos(df["pickup_day_of_week"] * 2 * math.pi / 7.0)
    df["distance_manhattan"] = (df["dolocation_id"] - df["pulocation_id"]).abs().astype(float)
    df["location_cluster"] = ((df["pulocation_id"] + df["dolocation_id"]) % 5).astype("int32")
    df["temporal_cluster"] = (df["pickup_hour"] // 6).astype("int32")
    df["actual_trip_distance"] = df["trip_distance"]
    df["actual_trip_duration_seconds"] = df["trip_duration_seconds"]
    df["estimate_level"] = "route_time"
    df["route_sample_count"] = 1
    return df[
        [
            "trip_id",
            "fare_amount",
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
            "pulocation_id",
            "dolocation_id",
            "actual_trip_distance",
            "actual_trip_duration_seconds",
            "estimate_level",
            "route_sample_count",
            "year_month",
        ]
    ]


def build_predictions(features: pd.DataFrame, lifecycle: pd.DataFrame) -> pd.DataFrame:
    df = features.merge(
        lifecycle[["trip_id", "started_event_id", "pickup_datetime"]],
        on="trip_id",
        how="left",
    )
    wobble = (((df["pulocation_id"] + df["dolocation_id"]) % 9) - 4) * 0.35
    df["predicted_fare_amount"] = (df["fare_amount"] + wobble).clip(lower=0)
    df["model_name"] = "XGB_NYC_Fare"
    df["model_version"] = "preview-v1"
    df["model_stage"] = "Production"
    df["prediction_timestamp"] = pd.to_datetime(df["pickup_datetime"]) + pd.to_timedelta(5, unit="s")
    return df.drop(columns=["fare_amount"])


def build_prediction_actuals(predictions: pd.DataFrame, lifecycle: pd.DataFrame) -> pd.DataFrame:
    actual = lifecycle[
        [
            "trip_id",
            "completed_event_id",
            "completed_ingest_timestamp",
            "pickup_datetime",
            "dropoff_datetime",
            "fare_amount",
            "total_amount",
            "trip_distance",
            "trip_duration_seconds",
            "year_month",
        ]
    ].rename(
        columns={
            "completed_ingest_timestamp": "actual_arrival_timestamp",
            "pickup_datetime": "actual_pickup_datetime",
            "fare_amount": "actual_fare_amount",
            "total_amount": "actual_total_amount",
            "trip_distance": "actual_trip_distance",
            "trip_duration_seconds": "actual_trip_duration_seconds",
        }
    )
    df = predictions.merge(actual, on=["trip_id", "year_month", "actual_trip_distance", "actual_trip_duration_seconds"], how="inner")
    df["prediction_error"] = df["predicted_fare_amount"] - df["actual_fare_amount"]
    df["absolute_error"] = df["prediction_error"].abs()
    df["squared_error"] = df["prediction_error"] ** 2
    df["label_delay_seconds"] = (
        pd.to_datetime(df["actual_arrival_timestamp"]) - pd.to_datetime(df["prediction_timestamp"])
    ).dt.total_seconds().clip(lower=0)
    df["joined_at"] = pd.Timestamp.utcnow().tz_localize(None)
    return df[
        [
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
    ]


def build_quality(actuals: pd.DataFrame) -> pd.DataFrame:
    df = actuals.copy()
    df["metric_date"] = pd.to_datetime(df["dropoff_datetime"]).dt.date
    grouped = (
        df.groupby(["metric_date", "model_name", "model_version"])
        .agg(
            prediction_count=("trip_id", "count"),
            mae=("absolute_error", "mean"),
            rmse=("squared_error", lambda s: math.sqrt(float(s.mean()))),
            bias=("prediction_error", "mean"),
            avg_predicted_fare=("predicted_fare_amount", "mean"),
            avg_actual_fare=("actual_fare_amount", "mean"),
            avg_label_delay_seconds=("label_delay_seconds", "mean"),
        )
        .reset_index()
    )
    grouped["metric_date"] = pd.to_datetime(grouped["metric_date"])
    grouped["updated_at"] = pd.Timestamp.utcnow().tz_localize(None)
    return grouped


def spark() -> SparkSession:
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("BootstrapLocalBILakehouse")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def write_delta(session: SparkSession, df: pd.DataFrame, rel: str, partition_by: list[str] | None = None) -> None:
    path = TARGET / rel
    sdf = session.createDataFrame(df)
    writer = sdf.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(str(path))
    print(f"[bootstrap-local-bi] wrote {len(df):,} rows -> {rel}")


def main() -> None:
    silver = read_delta_parquet_dir(SOURCE / "silver")
    lifecycle = build_lifecycle(silver)
    route_estimates = build_route_estimates(lifecycle)
    features = build_features(lifecycle)
    predictions = build_predictions(features, lifecycle)
    actuals = build_prediction_actuals(predictions, lifecycle)
    quality = build_quality(actuals)

    if TARGET.exists():
        shutil.rmtree(TARGET)
    session = spark()
    try:
        write_delta(session, lifecycle, "silver/nyc-taxi/trip_completed", ["year_month"])
        write_delta(session, lifecycle, "silver/nyc-taxi/trip_lifecycle", ["year_month"])
        write_delta(session, route_estimates, "gold/ml/route_estimates")
        write_delta(session, features, "gold/ml/features", ["year_month"])
        write_delta(session, predictions, "gold/ml/predictions", ["year_month"])
        write_delta(session, actuals, "gold/ml/prediction_actuals", ["year_month"])
        write_delta(session, quality, "gold/monitoring/model_quality_daily")
    finally:
        session.stop()


if __name__ == "__main__":
    main()
