"""
FastAPI Serving Layer — NYC Taxi Fare Prediction

Endpoints:
  GET  /health       → model status
  POST /predict      → fare prediction for a single trip
"""

import math
import numpy as np
import pandas as pd
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from app.config import MODEL_NAME, MODEL_STAGE, FEATURE_COLS
from app.model import load_model, get_model, get_model_version
from app.schemas import TripRequest, PredictionResponse, HealthResponse


# ---------------------------------------------------------------------------
# Lifespan: load model at startup
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    load_model()
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


# ---------------------------------------------------------------------------
# Feature engineering (mirrors feature_extractor.py but in pure Python/NumPy)
# ---------------------------------------------------------------------------

def _build_features(req: TripRequest) -> pd.DataFrame:
    two_pi = 2.0 * math.pi

    hour = req.pickup_hour
    dow  = req.pickup_day_of_week
    pu   = req.pulocation_id or 0
    do_  = req.dolocation_id or 0
    dur  = req.trip_duration_seconds

    speed = (req.trip_distance / (dur / 3600.0)) if dur > 0 else 0.0

    features = {
        "passenger_count":       req.passenger_count,
        "trip_distance":         req.trip_distance,
        "trip_duration_seconds": dur,
        "speed":                 speed,
        "pickup_hour":           hour,
        "pickup_day_of_week":    dow,
        "is_weekend":            1 if dow in (5, 6) else 0,
        "hour_sin":              math.sin(two_pi * hour / 24.0),
        "hour_cos":              math.cos(two_pi * hour / 24.0),
        "day_sin":               math.sin(two_pi * dow  / 7.0),
        "day_cos":               math.cos(two_pi * dow  / 7.0),
        "distance_manhattan":    abs(do_ - pu),
        "location_cluster":      (pu + do_) % 5,           # mirrors stream_predict proxy
        "temporal_cluster":      hour // 6,                 # 4 time-of-day buckets
    }

    return pd.DataFrame([features])[FEATURE_COLS]


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health", response_model=HealthResponse, tags=["Monitoring"])
def health():
    """Return model readiness and version info."""
    return HealthResponse(
        status="ok",
        model_name=MODEL_NAME,
        model_version=get_model_version(),
        model_stage=MODEL_STAGE,
    )


@app.post("/predict", response_model=PredictionResponse, tags=["Prediction"])
def predict(request: TripRequest):
    """
    Predict the fare amount for a NYC taxi trip.

    Provide trip details (distance, duration, pickup time, etc.)
    and receive a real-time fare estimate from the XGBoost model.
    """
    try:
        model = get_model()
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


@app.get("/", tags=["Info"])
def root():
    return {
        "service": "NYC Taxi Fare Prediction API",
        "docs":    "/docs",
        "health":  "/health",
        "predict": "POST /predict",
    }
