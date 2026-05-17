#!/usr/bin/env python3
"""
Demo the production-style streaming backend locally.

The script simulates the real stream_predict job without Kafka/Kubernetes:
  silver trip_started events -> route_estimates lookup -> MLflow model
  -> prediction log -> delayed actuals/quality summary.

It is meant for demos where you want to show that the model is used by an
always-on backend, not only by a manual web form.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from dataclasses import dataclass
from pathlib import Path

import mlflow
import mlflow.xgboost
import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


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


@dataclass
class RouteEstimate:
    distance: float
    duration_seconds: float
    sample_count: int
    estimate_level: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="_local_small_pipeline/silver_completed.parquet")
    parser.add_argument("--route-estimates", default="_local_small_pipeline/gold_route_estimates.parquet")
    parser.add_argument("--output-dir", default="_local_streaming_demo")
    parser.add_argument("--tracking-uri", default="http://127.0.0.1:5000")
    parser.add_argument("--model-name", default="XGB_NYC_Fare")
    parser.add_argument("--model-stage", default="Production")
    parser.add_argument("--events", type=int, default=30)
    parser.add_argument("--batch-size", type=int, default=5)
    parser.add_argument("--sleep-seconds", type=float, default=0.25)
    parser.add_argument("--start-offset", type=int, default=0)
    return parser.parse_args()


def load_model(tracking_uri: str, model_name: str, model_stage: str):
    mlflow.set_tracking_uri(tracking_uri)
    model_uri = f"models:/{model_name}/{model_stage}"
    print(f"[demo_stream] Loading model: {model_uri}")
    model = mlflow.xgboost.load_model(model_uri)

    client = mlflow.tracking.MlflowClient(tracking_uri=tracking_uri)
    versions = client.get_latest_versions(model_name, stages=[model_stage])
    model_version = versions[0].version if versions else "unknown"
    print(f"[demo_stream] Model ready: {model_name} v{model_version} ({model_stage})")
    return model, model_version


def load_route_indexes(route_path: Path):
    routes = pd.read_parquet(route_path)

    def make_estimate(row) -> RouteEstimate:
        return RouteEstimate(
            distance=float(row.estimated_trip_distance),
            duration_seconds=float(row.estimated_trip_duration_seconds),
            sample_count=int(row.sample_count),
            estimate_level=str(row.estimate_level),
        )

    exact = {}
    route_only = {}
    global_estimate = None

    for row in routes.itertuples(index=False):
        if row.estimate_level == "route_time":
            exact[
                (
                    int(row.pulocation_id),
                    int(row.dolocation_id),
                    int(row.pickup_hour),
                    int(row.pickup_day_of_week),
                )
            ] = make_estimate(row)
        elif row.estimate_level == "route":
            route_only[(int(row.pulocation_id), int(row.dolocation_id))] = make_estimate(row)
        elif row.estimate_level == "global":
            global_estimate = make_estimate(row)

    if global_estimate is None:
        raise RuntimeError("Route estimates do not contain a global fallback row")

    return exact, route_only, global_estimate


def resolve_route_estimate(row, exact, route_only, global_estimate: RouteEstimate) -> RouteEstimate:
    exact_key = (
        int(row.pulocation_id),
        int(row.dolocation_id),
        int(row.pickup_hour),
        int(row.pickup_day_of_week),
    )
    route_key = (int(row.pulocation_id), int(row.dolocation_id))
    return exact.get(exact_key) or route_only.get(route_key) or global_estimate


def build_feature_row(row, estimate: RouteEstimate) -> dict:
    two_pi = 2.0 * math.pi
    duration_hours = estimate.duration_seconds / 3600.0
    pickup_hour = int(row.pickup_hour)
    pickup_day = int(row.pickup_day_of_week)
    pulocation_id = int(row.pulocation_id)
    dolocation_id = int(row.dolocation_id)

    return {
        "passenger_count": int(row.passenger_count),
        "estimated_trip_distance": estimate.distance,
        "estimated_trip_duration_seconds": estimate.duration_seconds,
        "estimated_speed": estimate.distance / duration_hours if duration_hours > 0 else 0.0,
        "pickup_hour": pickup_hour,
        "pickup_day_of_week": pickup_day,
        "is_weekend": 1 if pickup_day in (5, 6) else 0,
        "hour_sin": math.sin(two_pi * pickup_hour / 24.0),
        "hour_cos": math.cos(two_pi * pickup_hour / 24.0),
        "day_sin": math.sin(two_pi * pickup_day / 7.0),
        "day_cos": math.cos(two_pi * pickup_day / 7.0),
        "distance_manhattan": abs(dolocation_id - pulocation_id),
        "location_cluster": (pulocation_id + dolocation_id) % 5,
        "temporal_cluster": pickup_hour // 6,
    }


def iter_micro_batches(df: pd.DataFrame, batch_size: int):
    for batch_id, start in enumerate(range(0, len(df), batch_size)):
        yield batch_id, df.iloc[start : start + batch_size].copy()


def append_jsonl(path: Path, records: list[dict]) -> None:
    with path.open("a", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, default=str) + "\n")


def main() -> None:
    args = parse_args()
    input_path = Path(args.input).resolve()
    route_path = Path(args.route_estimates).resolve()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    prediction_log_path = output_dir / "predictions.jsonl"
    predictions_csv_path = output_dir / "predictions.csv"
    actuals_csv_path = output_dir / "prediction_actuals.csv"
    quality_path = output_dir / "model_quality_summary.json"

    prediction_log_path.write_text("", encoding="utf-8")

    model, model_version = load_model(args.tracking_uri, args.model_name, args.model_stage)
    exact, route_only, global_estimate = load_route_indexes(route_path)

    silver = pd.read_parquet(input_path)
    silver["pickup_datetime"] = pd.to_datetime(silver["pickup_datetime"])
    silver = silver.sort_values("pickup_datetime").iloc[
        args.start_offset : args.start_offset + args.events
    ].copy()
    silver["pickup_hour"] = silver["pickup_datetime"].dt.hour.astype(int)
    silver["pickup_day_of_week"] = silver["pickup_datetime"].dt.dayofweek.astype(int)

    print(
        "[demo_stream] Starting backend stream: "
        f"events={len(silver)}, batch_size={args.batch_size}, output={output_dir}"
    )
    print("[demo_stream] This simulates an always-on service; no web form calls are made.")

    all_predictions: list[dict] = []
    all_actuals: list[dict] = []

    for batch_id, batch in iter_micro_batches(silver, args.batch_size):
        features = []
        estimates = []
        for row in batch.itertuples(index=False):
            estimate = resolve_route_estimate(row, exact, route_only, global_estimate)
            estimates.append(estimate)
            features.append(build_feature_row(row, estimate))

        X = pd.DataFrame(features)[FEATURE_COLS].replace([np.inf, -np.inf], 0).fillna(0)
        fares = np.maximum(model.predict(X).astype(float), 0.0)
        prediction_ts = pd.Timestamp.utcnow()

        prediction_records = []
        actual_records = []
        for row, estimate, predicted_fare in zip(batch.itertuples(index=False), estimates, fares):
            prediction = {
                "trip_id": row.trip_id,
                "started_event_id": row.event_id,
                "pickup_datetime": row.pickup_datetime,
                "pulocation_id": int(row.pulocation_id),
                "dolocation_id": int(row.dolocation_id),
                "passenger_count": int(row.passenger_count),
                "estimated_trip_distance": round(estimate.distance, 3),
                "estimated_trip_duration_seconds": round(estimate.duration_seconds, 1),
                "estimate_level": estimate.estimate_level,
                "route_sample_count": estimate.sample_count,
                "predicted_fare_amount": round(float(predicted_fare), 2),
                "model_name": args.model_name,
                "model_version": model_version,
                "model_stage": args.model_stage,
                "prediction_timestamp": prediction_ts,
                "year_month": row.year_month,
            }
            actual = {
                **prediction,
                "completed_event_id": row.event_id,
                "dropoff_datetime": row.dropoff_datetime,
                "actual_fare_amount": round(float(row.fare_amount), 2),
                "prediction_error": round(float(predicted_fare - row.fare_amount), 2),
                "absolute_error": round(abs(float(predicted_fare - row.fare_amount)), 2),
            }
            prediction_records.append(prediction)
            actual_records.append(actual)

        append_jsonl(prediction_log_path, prediction_records)
        all_predictions.extend(prediction_records)
        all_actuals.extend(actual_records)

        avg_pred = sum(item["predicted_fare_amount"] for item in prediction_records) / len(prediction_records)
        first = prediction_records[0]
        print(
            f"[demo_stream] micro_batch={batch_id:03d} "
            f"events={len(prediction_records)} "
            f"avg_predicted_fare=${avg_pred:.2f} "
            f"model=v{model_version} "
            f"sample_trip={first['trip_id'][:10]} "
            f"route={first['pulocation_id']}->{first['dolocation_id']} "
            f"fare=${first['predicted_fare_amount']:.2f}"
        )

        if args.sleep_seconds > 0:
            time.sleep(args.sleep_seconds)

    predictions = pd.DataFrame(all_predictions)
    actuals = pd.DataFrame(all_actuals)
    predictions.to_csv(predictions_csv_path, index=False)
    actuals.to_csv(actuals_csv_path, index=False)

    y_true = actuals["actual_fare_amount"].astype(float)
    y_pred = actuals["predicted_fare_amount"].astype(float)
    quality = {
        "prediction_count": int(len(actuals)),
        "model_name": args.model_name,
        "model_version": model_version,
        "model_stage": args.model_stage,
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "rmse": float(mean_squared_error(y_true, y_pred) ** 0.5),
        "r2": float(r2_score(y_true, y_pred)) if len(actuals) > 1 else None,
        "predictions_jsonl": str(prediction_log_path),
        "predictions_csv": str(predictions_csv_path),
        "prediction_actuals_csv": str(actuals_csv_path),
    }
    quality_path.write_text(json.dumps(quality, indent=2), encoding="utf-8")

    print("[demo_stream] Stream completed")
    print(json.dumps(quality, indent=2))


if __name__ == "__main__":
    main()
