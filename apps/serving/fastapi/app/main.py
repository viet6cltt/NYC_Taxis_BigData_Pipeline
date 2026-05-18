"""
FastAPI Serving Layer — NYC Taxi Fare Prediction

Endpoints:
  GET  /health       → model status
  POST /predict      → fare prediction for a single trip
"""

import math
import csv
import json
import numpy as np
import pandas as pd
from contextlib import asynccontextmanager
from functools import lru_cache
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.config import (
    FEATURE_COLS,
    LOAD_MODEL_ON_STARTUP,
    MODEL_NAME,
    MODEL_STAGE,
    N_LOCATION_CLUSTERS,
    N_TEMPORAL_CLUSTERS,
)
from app.model import load_model, get_model, get_model_version
from app.schemas import TripRequest, PredictionResponse, HealthResponse

APP_DIR = Path(__file__).resolve().parent
STATIC_DIR = APP_DIR / "static"
ZONE_LOOKUP_FILE = "taxi_zone_lookup.csv"
STREAM_DEMO_DIR = "_local_streaming_demo"
LOCAL_ROUTE_ESTIMATES_FILE = "_local_small_pipeline/gold_route_estimates.parquet"
LOCAL_ROUTE_ESTIMATES_DELTA_DIR = "_local_delta_store/lakehouse/gold/ml/route_estimates"


# ---------------------------------------------------------------------------
# Lifespan: load model at startup
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    if LOAD_MODEL_ON_STARTUP:
        try:
            load_model()
            app.state.model_startup_error = None
        except Exception as exc:
            app.state.model_startup_error = str(exc)
            print(f"[fastapi] Model unavailable at startup: {exc}")
    else:
        app.state.model_startup_error = "Model loading disabled by LOAD_MODEL_ON_STARTUP"
        print("[fastapi] Model loading skipped at startup")
    yield


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="NYC Taxi Fare Prediction API",
    description=(
        "Real-time fare prediction powered by XGBoost trained on NYC Yellow Taxi data. "
        "Model served from MLflow Model Registry."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


# ---------------------------------------------------------------------------
# Feature engineering (mirrors feature_extractor.py but in pure Python/NumPy)
# ---------------------------------------------------------------------------

def _build_features(req: TripRequest) -> pd.DataFrame:
    two_pi = 2.0 * math.pi

    hour = req.pickup_hour
    dow  = req.pickup_day_of_week
    pu   = req.pulocation_id or 0
    do_  = req.dolocation_id or 0
    dur  = req.estimated_trip_duration_seconds
    temporal_bucket_size = max(1, 24 // N_TEMPORAL_CLUSTERS)

    speed = (req.estimated_trip_distance / (dur / 3600.0)) if dur > 0 else 0.0

    features = {
        "passenger_count":       req.passenger_count,
        "estimated_trip_distance": req.estimated_trip_distance,
        "estimated_trip_duration_seconds": dur,
        "estimated_speed":       speed,
        "pickup_hour":           hour,
        "pickup_day_of_week":    dow,
        "is_weekend":            1 if dow in (5, 6) else 0,
        "hour_sin":              math.sin(two_pi * hour / 24.0),
        "hour_cos":              math.cos(two_pi * hour / 24.0),
        "day_sin":               math.sin(two_pi * dow  / 7.0),
        "day_cos":               math.cos(two_pi * dow  / 7.0),
        "distance_manhattan":    abs(do_ - pu),
        "location_cluster":      (pu + do_) % N_LOCATION_CLUSTERS,
        "temporal_cluster":      hour // temporal_bucket_size,
    }

    return pd.DataFrame([features])[FEATURE_COLS]


def _find_zone_lookup() -> Path | None:
    """Find TLC zone lookup when running from repo root or packaged app."""
    for parent in (APP_DIR, *APP_DIR.parents):
        candidate = parent / "data" / ZONE_LOOKUP_FILE
        if candidate.exists():
            return candidate
    return None


@lru_cache(maxsize=1)
def _load_zones() -> list[dict]:
    path = _find_zone_lookup()

    if path is None:
        return [
            {"id": 132, "borough": "Queens", "zone": "JFK Airport", "service_zone": "Airports"},
            {"id": 138, "borough": "Queens", "zone": "LaGuardia Airport", "service_zone": "Airports"},
            {"id": 161, "borough": "Manhattan", "zone": "Midtown Center", "service_zone": "Yellow Zone"},
            {"id": 186, "borough": "Manhattan", "zone": "Penn Station/Madison Sq West", "service_zone": "Yellow Zone"},
            {"id": 230, "borough": "Manhattan", "zone": "Times Sq/Theatre District", "service_zone": "Yellow Zone"},
            {"id": 236, "borough": "Manhattan", "zone": "Upper East Side North", "service_zone": "Yellow Zone"},
            {"id": 237, "borough": "Manhattan", "zone": "Upper East Side South", "service_zone": "Yellow Zone"},
            {"id": 211, "borough": "Manhattan", "zone": "SoHo", "service_zone": "Yellow Zone"},
            {"id": 87, "borough": "Manhattan", "zone": "Financial District North", "service_zone": "Yellow Zone"},
            {"id": 234, "borough": "Manhattan", "zone": "Union Sq", "service_zone": "Yellow Zone"},
        ]

    zones: list[dict] = []
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            zones.append(
                {
                    "id": int(row["LocationID"]),
                    "borough": row["Borough"],
                    "zone": row["Zone"],
                    "service_zone": row["service_zone"],
                }
            )

    return zones


def _find_repo_root() -> Path:
    for parent in (APP_DIR, *APP_DIR.parents):
        if (parent / "README.md").exists() and (parent / "apps").exists():
            return parent
    return APP_DIR


def _find_route_estimates_path() -> Path | None:
    repo_root = _find_repo_root()
    candidates = [
        repo_root / LOCAL_ROUTE_ESTIMATES_FILE,
        repo_root / LOCAL_ROUTE_ESTIMATES_DELTA_DIR,
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


@lru_cache(maxsize=1)
def _load_route_estimates() -> pd.DataFrame:
    path = _find_route_estimates_path()
    if path is None:
        return pd.DataFrame()

    if path.is_dir():
        parquet_files = sorted(path.glob("*.parquet"))
        if not parquet_files:
            return pd.DataFrame()
        return pd.concat([pd.read_parquet(file) for file in parquet_files], ignore_index=True)

    return pd.read_parquet(path)


def _route_estimate_response(row: pd.Series) -> dict:
    duration_seconds = float(row["estimated_trip_duration_seconds"])
    return {
        "estimate_level": row["estimate_level"],
        "pulocation_id": int(row["pulocation_id"]) if not pd.isna(row["pulocation_id"]) else None,
        "dolocation_id": int(row["dolocation_id"]) if not pd.isna(row["dolocation_id"]) else None,
        "pickup_hour": int(row["pickup_hour"]) if "pickup_hour" in row and not pd.isna(row["pickup_hour"]) else None,
        "pickup_day_of_week": int(row["pickup_day_of_week"]) if "pickup_day_of_week" in row and not pd.isna(row["pickup_day_of_week"]) else None,
        "sample_count": int(row["sample_count"]) if not pd.isna(row["sample_count"]) else 0,
        "estimated_trip_distance": round(float(row["estimated_trip_distance"]), 2),
        "estimated_trip_duration_seconds": round(duration_seconds, 0),
        "duration_minutes": max(1, round(duration_seconds / 60.0)),
        "source": "gold_route_estimates",
    }


def _lookup_route_estimate(
    pulocation_id: int,
    dolocation_id: int,
    pickup_hour: int,
    pickup_day_of_week: int,
) -> dict | None:
    estimates = _load_route_estimates()
    if estimates.empty:
        return None

    route_time = estimates[
        (estimates["estimate_level"] == "route_time")
        & (estimates["pulocation_id"].astype("Int64") == int(pulocation_id))
        & (estimates["dolocation_id"].astype("Int64") == int(dolocation_id))
        & (estimates["pickup_hour"].astype("Int64") == int(pickup_hour))
        & (estimates["pickup_day_of_week"].astype("Int64") == int(pickup_day_of_week))
    ]
    if not route_time.empty:
        return _route_estimate_response(route_time.sort_values("sample_count", ascending=False).iloc[0])

    route = estimates[
        (estimates["estimate_level"] == "route")
        & (estimates["pulocation_id"].astype("Int64") == int(pulocation_id))
        & (estimates["dolocation_id"].astype("Int64") == int(dolocation_id))
    ]
    if not route.empty:
        return _route_estimate_response(route.sort_values("sample_count", ascending=False).iloc[0])

    global_estimate = estimates[estimates["estimate_level"] == "global"]
    if not global_estimate.empty:
        return _route_estimate_response(global_estimate.sort_values("sample_count", ascending=False).iloc[0])

    return None


def _clean_json_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    return value


def _records_from_csv(path: Path, limit: int = 100) -> list[dict]:
    if not path.exists():
        return []
    df = pd.read_csv(path).head(limit)
    return [
        {key: _clean_json_value(value) for key, value in row.items()}
        for row in df.to_dict(orient="records")
    ]


def _load_stream_demo_data() -> dict:
    demo_dir = _find_repo_root() / STREAM_DEMO_DIR
    quality_path = demo_dir / "model_quality_summary.json"
    predictions_path = demo_dir / "predictions.csv"
    actuals_path = demo_dir / "prediction_actuals.csv"

    quality = {}
    if quality_path.exists():
        quality = json.loads(quality_path.read_text(encoding="utf-8"))

    predictions = _records_from_csv(predictions_path)
    actuals = _records_from_csv(actuals_path)

    return {
        "available": predictions_path.exists() and quality_path.exists(),
        "demo_dir": str(demo_dir),
        "quality": quality,
        "predictions": predictions,
        "actuals": actuals,
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health", response_model=HealthResponse, tags=["Monitoring"])
def health():
    """Return model readiness and version info."""
    try:
        get_model()
        status = "ok"
    except RuntimeError:
        status = "degraded"

    return HealthResponse(
        status=status,
        model_name=MODEL_NAME,
        model_version=get_model_version(),
        model_stage=MODEL_STAGE,
    )


@app.get("/zones", tags=["Reference Data"])
def zones():
    """Return TLC taxi zones for the prediction UI."""
    items = _load_zones()
    return {
        "count": len(items),
        "zones": [
            {
                **item,
                "label": f"{item['borough']} - {item['zone']}",
            }
            for item in items
        ],
    }


@app.get("/route-estimate", tags=["Reference Data"])
def route_estimate(
    pulocation_id: int = Query(..., gt=0),
    dolocation_id: int = Query(..., gt=0),
    pickup_hour: int = Query(..., ge=0, le=23),
    pickup_day_of_week: int = Query(..., ge=0, le=6),
):
    """Return historical distance/duration estimate for a pickup/dropoff route."""
    estimate = _lookup_route_estimate(
        pulocation_id=pulocation_id,
        dolocation_id=dolocation_id,
        pickup_hour=pickup_hour,
        pickup_day_of_week=pickup_day_of_week,
    )
    if estimate is None:
        raise HTTPException(status_code=404, detail="Route estimates unavailable")
    return estimate


@app.get("/stream-demo/data", tags=["Streaming Demo"])
def stream_demo_data():
    """Return local streaming backend demo predictions and quality metrics."""
    return _load_stream_demo_data()


@app.post("/predict", response_model=PredictionResponse, tags=["Prediction"])
def predict(request: TripRequest):
    """
    Predict the fare amount for a NYC taxi trip.

    Provide trip details (distance, duration, pickup time, etc.)
    and receive a real-time fare estimate from the XGBoost model.
    """
    try:
        try:
            model = get_model()
        except RuntimeError:
            try:
                load_model()
                model = get_model()
            except Exception as e:
                raise HTTPException(status_code=503, detail=f"Model unavailable: {str(e)}")

        X = _build_features(request)
        X = X.replace([np.inf, -np.inf], 0).fillna(0)
        predicted = float(model.predict(X)[0])

        if predicted < 0:
            predicted = 0.0

        return PredictionResponse(
            predicted_fare=round(predicted, 2),
            model_name=MODEL_NAME,
            model_version=get_model_version(),
        )
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction failed: {str(e)}")


@app.get("/api/info", tags=["Info"])
def api_info():
    return {
        "service": "NYC Taxi Fare Prediction API",
        "ui":      "/",
        "docs":    "/docs",
        "health":  "/health",
        "zones":   "/zones",
        "route_estimate": "GET /route-estimate",
        "stream_demo": "/stream-demo",
        "stream_demo_data": "/stream-demo/data",
        "predict": "POST /predict",
    }


@app.get("/", tags=["UI"], include_in_schema=False)
def root():
    index_path = STATIC_DIR / "index.html"
    if index_path.exists():
        return FileResponse(index_path)
    return api_info()


@app.get("/stream-demo", tags=["UI"], include_in_schema=False)
def stream_demo():
    page_path = STATIC_DIR / "stream-demo.html"
    if page_path.exists():
        return FileResponse(page_path)
    return _load_stream_demo_data()
