#!/usr/bin/env python3
"""
Run a small local NYC Taxi pipeline and train/register an XGBoost model in MLflow.

This runner is intentionally lightweight:
  raw parquet -> bronze-like parquet -> silver completed parquet
  -> route estimates -> gold features -> XGBoost -> MLflow model registry

It avoids Kubernetes/S3 so the pipeline can be exercised quickly on a laptop or
workspace while preserving the same serving feature contract.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path

import mlflow
import mlflow.xgboost
import numpy as np
import pandas as pd
import xgboost as xgb
from mlflow.models.signature import infer_signature
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split


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

RAW_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "Airport_fee",
]

TARGET_COL = "fare_amount"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="data/yellow_data/2024/yellow_tripdata_2024-01.parquet")
    parser.add_argument("--rows", type=int, default=10000)
    parser.add_argument("--output-dir", default="_local_small_pipeline")
    parser.add_argument("--tracking-uri", default="http://127.0.0.1:5000")
    parser.add_argument("--experiment-name", default="NYC_Taxi_Fare_Prediction")
    parser.add_argument("--model-name", default="XGB_NYC_Fare")
    parser.add_argument("--promote", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--n-estimators", type=int, default=80)
    parser.add_argument("--max-depth", type=int, default=5)
    parser.add_argument("--learning-rate", type=float, default=0.08)
    parser.add_argument("--test-size", type=float, default=0.2)
    parser.add_argument("--random-state", type=int, default=42)
    return parser.parse_args()


def stable_hash(*parts: object) -> str:
    text = "||".join("" if part is None else str(part) for part in parts)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def read_raw(path: Path, row_limit: int) -> pd.DataFrame:
    print(f"[small_pipeline] Reading raw parquet: {path}")
    raw = pd.read_parquet(path, columns=RAW_COLUMNS)
    raw = raw.head(row_limit).copy()
    print(f"[small_pipeline] Raw rows: {len(raw):,}")
    return raw


def build_bronze(raw: pd.DataFrame, source_file: str) -> pd.DataFrame:
    print("[small_pipeline] Building bronze completed events")
    bronze = raw.rename(
        columns={
            "VendorID": "vendor_id",
            "tpep_pickup_datetime": "pickup_datetime",
            "tpep_dropoff_datetime": "dropoff_datetime",
            "RatecodeID": "rate_code_id",
            "PULocationID": "pulocation_id",
            "DOLocationID": "dolocation_id",
            "Airport_fee": "airport_fee",
        }
    ).copy()

    bronze["event_type"] = "trip_completed"
    bronze["schema_version"] = "v1"
    bronze["source_file"] = source_file
    bronze["ingest_mode"] = "batch"
    bronze["ingest_timestamp"] = pd.Timestamp.utcnow()
    bronze["event_time"] = bronze["dropoff_datetime"]
    bronze["trip_date"] = bronze["pickup_datetime"].dt.strftime("%Y-%m-%d")

    bronze["trip_id"] = [
        stable_hash(
            source_file,
            row.vendor_id,
            row.pickup_datetime,
            row.dropoff_datetime,
            row.pulocation_id,
            row.dolocation_id,
            row.fare_amount,
            row.total_amount,
        )
        for row in bronze.itertuples(index=False)
    ]
    bronze["event_id"] = [stable_hash("v1", "trip_completed", trip_id) for trip_id in bronze["trip_id"]]

    return bronze


def payment_type_desc(value: float | int | None) -> str:
    mapping = {
        1: "credit_card",
        2: "cash",
        3: "no_charge",
        4: "dispute",
        5: "unknown",
        6: "voided_trip",
    }
    if pd.isna(value):
        return "other"
    return mapping.get(int(value), "other")


def build_silver_completed(bronze: pd.DataFrame) -> pd.DataFrame:
    print("[small_pipeline] Cleaning silver completed trips")
    silver = bronze.copy()
    silver["pickup_datetime"] = pd.to_datetime(silver["pickup_datetime"], errors="coerce")
    silver["dropoff_datetime"] = pd.to_datetime(silver["dropoff_datetime"], errors="coerce")
    silver["trip_duration_seconds"] = (
        silver["dropoff_datetime"] - silver["pickup_datetime"]
    ).dt.total_seconds()

    filters = (
        silver["event_id"].notna()
        & silver["trip_id"].notna()
        & silver["pickup_datetime"].notna()
        & silver["dropoff_datetime"].notna()
        & (silver["dropoff_datetime"] > silver["pickup_datetime"])
        & silver["trip_distance"].notna()
        & silver["trip_distance"].between(0.05, 100.0)
        & silver["trip_duration_seconds"].between(60, 14400)
        & silver["fare_amount"].notna()
        & silver["fare_amount"].between(2.5, 300.0)
        & silver["total_amount"].notna()
        & silver["total_amount"].between(0, 500.0)
        & silver["passenger_count"].notna()
        & silver["passenger_count"].between(1, 6)
    )
    silver = silver.loc[filters].drop_duplicates("event_id").copy()
    duration_hours = silver["trip_duration_seconds"] / 3600.0
    silver = silver.loc[(silver["trip_distance"] / duration_hours).between(1.0, 80.0)].copy()

    silver["trip_hour"] = silver["pickup_datetime"].dt.hour.astype("int64")
    silver["year_month"] = silver["pickup_datetime"].dt.strftime("%Y-%m")
    silver["payment_type_desc"] = silver["payment_type"].map(payment_type_desc)
    silver["passenger_count"] = silver["passenger_count"].astype("int64")
    silver["pulocation_id"] = silver["pulocation_id"].astype("int64")
    silver["dolocation_id"] = silver["dolocation_id"].astype("int64")
    silver["trip_duration_seconds"] = silver["trip_duration_seconds"].astype("int64")

    print(f"[small_pipeline] Silver completed rows: {len(silver):,}")
    return silver


def add_temporal_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["pickup_hour"] = df["pickup_datetime"].dt.hour.astype("int64")
    df["pickup_day_of_week"] = df["pickup_datetime"].dt.dayofweek.astype("int64")
    df["is_weekend"] = df["pickup_day_of_week"].isin([5, 6]).astype("int64")
    df["year_month"] = df["pickup_datetime"].dt.strftime("%Y-%m")
    return df


def build_route_estimates(silver: pd.DataFrame) -> pd.DataFrame:
    print("[small_pipeline] Building route estimates")
    completed = add_temporal_features(silver)

    def aggregate(group_cols: list[str], estimate_level: str) -> pd.DataFrame:
        if group_cols:
            grouped = (
                completed.groupby(group_cols, dropna=False)
                .agg(
                    sample_count=("trip_id", "size"),
                    avg_trip_distance=("trip_distance", "mean"),
                    median_trip_distance=("trip_distance", "median"),
                    avg_trip_duration_seconds=("trip_duration_seconds", "mean"),
                    median_trip_duration_seconds=("trip_duration_seconds", "median"),
                )
                .reset_index()
            )
        else:
            grouped = pd.DataFrame(
                [
                    {
                        "sample_count": len(completed),
                        "avg_trip_distance": completed["trip_distance"].mean(),
                        "median_trip_distance": completed["trip_distance"].median(),
                        "avg_trip_duration_seconds": completed["trip_duration_seconds"].mean(),
                        "median_trip_duration_seconds": completed["trip_duration_seconds"].median(),
                    }
                ]
            )

        grouped["estimate_level"] = estimate_level
        for name in ["pulocation_id", "dolocation_id", "pickup_hour", "pickup_day_of_week"]:
            if name not in grouped.columns:
                grouped[name] = np.nan
        return grouped

    route_estimates = pd.concat(
        [
            aggregate(["pulocation_id", "dolocation_id", "pickup_hour", "pickup_day_of_week"], "route_time"),
            aggregate(["pulocation_id", "dolocation_id"], "route"),
            aggregate([], "global"),
        ],
        ignore_index=True,
    )
    route_estimates["estimated_trip_distance"] = route_estimates["median_trip_distance"].astype(float)
    route_estimates["estimated_trip_duration_seconds"] = route_estimates[
        "median_trip_duration_seconds"
    ].astype(float)
    route_estimates["updated_at"] = pd.Timestamp.utcnow()
    print(f"[small_pipeline] Route estimate rows: {len(route_estimates):,}")
    return route_estimates


def build_gold_features(silver: pd.DataFrame, route_estimates: pd.DataFrame) -> pd.DataFrame:
    print("[small_pipeline] Building gold training features")
    df = add_temporal_features(silver)

    exact = route_estimates[route_estimates["estimate_level"] == "route_time"][
        [
            "pulocation_id",
            "dolocation_id",
            "pickup_hour",
            "pickup_day_of_week",
            "estimated_trip_distance",
            "estimated_trip_duration_seconds",
            "sample_count",
        ]
    ].rename(
        columns={
            "estimated_trip_distance": "exact_estimated_trip_distance",
            "estimated_trip_duration_seconds": "exact_estimated_trip_duration_seconds",
            "sample_count": "exact_sample_count",
        }
    )
    route = route_estimates[route_estimates["estimate_level"] == "route"][
        [
            "pulocation_id",
            "dolocation_id",
            "estimated_trip_distance",
            "estimated_trip_duration_seconds",
            "sample_count",
        ]
    ].rename(
        columns={
            "estimated_trip_distance": "route_estimated_trip_distance",
            "estimated_trip_duration_seconds": "route_estimated_trip_duration_seconds",
            "sample_count": "route_sample_count_raw",
        }
    )
    global_row = route_estimates[route_estimates["estimate_level"] == "global"].iloc[0]

    df = df.merge(
        exact,
        on=["pulocation_id", "dolocation_id", "pickup_hour", "pickup_day_of_week"],
        how="left",
    )
    df = df.merge(route, on=["pulocation_id", "dolocation_id"], how="left")
    df["estimated_trip_distance"] = (
        df["exact_estimated_trip_distance"]
        .fillna(df["route_estimated_trip_distance"])
        .fillna(float(global_row["estimated_trip_distance"]))
    )
    df["estimated_trip_duration_seconds"] = (
        df["exact_estimated_trip_duration_seconds"]
        .fillna(df["route_estimated_trip_duration_seconds"])
        .fillna(float(global_row["estimated_trip_duration_seconds"]))
    )
    df["estimate_level"] = np.select(
        [df["exact_sample_count"].notna(), df["route_sample_count_raw"].notna()],
        ["route_time", "route"],
        default="global",
    )
    df["route_sample_count"] = (
        df["exact_sample_count"]
        .fillna(df["route_sample_count_raw"])
        .fillna(float(global_row["sample_count"]))
    )

    two_pi = 2.0 * math.pi
    df["estimated_speed"] = df["estimated_trip_distance"] / (
        df["estimated_trip_duration_seconds"] / 3600.0
    )
    df["hour_sin"] = np.sin(df["pickup_hour"] * two_pi / 24.0)
    df["hour_cos"] = np.cos(df["pickup_hour"] * two_pi / 24.0)
    df["day_sin"] = np.sin(df["pickup_day_of_week"] * two_pi / 7.0)
    df["day_cos"] = np.cos(df["pickup_day_of_week"] * two_pi / 7.0)
    df["distance_manhattan"] = (df["dolocation_id"] - df["pulocation_id"]).abs()
    df["location_cluster"] = ((df["pulocation_id"] + df["dolocation_id"]) % 5).astype("int64")
    df["temporal_cluster"] = (df["pickup_hour"] // 6).astype("int64")
    df["actual_trip_distance"] = df["trip_distance"]
    df["actual_trip_duration_seconds"] = df["trip_duration_seconds"]

    feature_filters = (
        df["estimated_trip_distance"].between(0.05, 100.0)
        & df["estimated_trip_duration_seconds"].between(60, 14400)
        & df["estimated_speed"].between(1.0, 80.0)
    )
    df = df.loc[feature_filters].copy()

    cols = [
        "trip_id",
        TARGET_COL,
        *FEATURE_COLS,
        "pulocation_id",
        "dolocation_id",
        "actual_trip_distance",
        "actual_trip_duration_seconds",
        "estimate_level",
        "route_sample_count",
        "year_month",
    ]
    gold = df[cols].replace([np.inf, -np.inf], np.nan).dropna(subset=[TARGET_COL, *FEATURE_COLS])
    print(f"[small_pipeline] Gold feature rows: {len(gold):,}")
    return gold


def train_and_log(
    gold: pd.DataFrame,
    tracking_uri: str,
    experiment_name: str,
    model_name: str,
    promote: bool,
    n_estimators: int,
    max_depth: int,
    learning_rate: float,
    test_size: float,
    random_state: int,
) -> dict:
    print("[small_pipeline] Training XGBoost and logging to MLflow")
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment_name)

    X = gold[FEATURE_COLS].astype(float)
    y = gold[TARGET_COL].astype(float)
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=test_size,
        random_state=random_state,
    )

    model = xgb.XGBRegressor(
        n_estimators=n_estimators,
        max_depth=max_depth,
        learning_rate=learning_rate,
        subsample=0.85,
        colsample_bytree=0.85,
        objective="reg:squarederror",
        eval_metric="rmse",
        random_state=random_state,
        n_jobs=2,
    )

    with mlflow.start_run(run_name="small-local-xgboost") as run:
        model.fit(X_train, y_train)
        train_pred = model.predict(X_train)
        test_pred = model.predict(X_test)

        metrics = {
            "train_r2": float(r2_score(y_train, train_pred)),
            "train_rmse": float(mean_squared_error(y_train, train_pred) ** 0.5),
            "train_mae": float(mean_absolute_error(y_train, train_pred)),
            "test_r2": float(r2_score(y_test, test_pred)),
            "test_rmse": float(mean_squared_error(y_test, test_pred) ** 0.5),
            "test_mae": float(mean_absolute_error(y_test, test_pred)),
        }
        params = {
            "model_type": "XGBoost",
            "pipeline_mode": "small_local",
            "n_features": len(FEATURE_COLS),
            "train_size": len(X_train),
            "test_size": len(X_test),
            "n_estimators": n_estimators,
            "max_depth": max_depth,
            "learning_rate": learning_rate,
            "random_state": random_state,
        }
        mlflow.log_params(params)
        mlflow.log_metrics(metrics)

        importance = pd.DataFrame(
            {"feature": FEATURE_COLS, "importance": model.feature_importances_}
        ).sort_values("importance", ascending=False)
        importance_path = Path("/tmp/small_local_feature_importance.csv")
        importance.to_csv(importance_path, index=False)
        mlflow.log_artifact(str(importance_path), artifact_path="feature_importance")

        input_example = X.head(10)
        signature = infer_signature(input_example, model.predict(input_example))
        mlflow.xgboost.log_model(
            model,
            artifact_path="model",
            signature=signature,
            input_example=input_example,
            registered_model_name=model_name,
        )

        run_id = run.info.run_id

    model_version = "unknown"
    if promote:
        client = mlflow.tracking.MlflowClient(tracking_uri=tracking_uri)
        versions = client.search_model_versions(f"name = '{model_name}'")
        latest = max(versions, key=lambda version: int(version.version), default=None)
        if latest is not None:
            model_version = latest.version
            client.transition_model_version_stage(
                name=model_name,
                version=model_version,
                stage="Production",
                archive_existing_versions=True,
            )
            print(f"[small_pipeline] Promoted {model_name} v{model_version} to Production")

    print(
        "[small_pipeline] Metrics: "
        f"test_r2={metrics['test_r2']:.4f}, "
        f"test_rmse={metrics['test_rmse']:.4f}, "
        f"test_mae={metrics['test_mae']:.4f}"
    )
    return {"run_id": run_id, "model_version": model_version, "metrics": metrics}


def main() -> None:
    args = parse_args()
    input_path = Path(args.input).resolve()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    raw = read_raw(input_path, args.rows)
    bronze = build_bronze(raw, source_file=str(input_path))
    silver = build_silver_completed(bronze)
    route_estimates = build_route_estimates(silver)
    gold = build_gold_features(silver, route_estimates)

    if len(gold) < 100:
        raise RuntimeError(f"Not enough rows for training after filters: {len(gold)}")

    bronze_path = output_dir / "bronze_completed.parquet"
    silver_path = output_dir / "silver_completed.parquet"
    route_path = output_dir / "gold_route_estimates.parquet"
    features_path = output_dir / "gold_features.parquet"
    summary_path = output_dir / "summary.json"

    bronze.to_parquet(bronze_path, index=False)
    silver.to_parquet(silver_path, index=False)
    route_estimates.to_parquet(route_path, index=False)
    gold.to_parquet(features_path, index=False)

    result = train_and_log(
        gold=gold,
        tracking_uri=args.tracking_uri,
        experiment_name=args.experiment_name,
        model_name=args.model_name,
        promote=args.promote,
        n_estimators=args.n_estimators,
        max_depth=args.max_depth,
        learning_rate=args.learning_rate,
        test_size=args.test_size,
        random_state=args.random_state,
    )

    summary = {
        "input": str(input_path),
        "rows_requested": args.rows,
        "bronze_rows": int(len(bronze)),
        "silver_completed_rows": int(len(silver)),
        "route_estimate_rows": int(len(route_estimates)),
        "gold_feature_rows": int(len(gold)),
        "outputs": {
            "bronze_completed": str(bronze_path),
            "silver_completed": str(silver_path),
            "gold_route_estimates": str(route_path),
            "gold_features": str(features_path),
        },
        "mlflow": {
            "tracking_uri": args.tracking_uri,
            "experiment_name": args.experiment_name,
            "model_name": args.model_name,
            **result,
        },
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"[small_pipeline] Summary written to {summary_path}")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
