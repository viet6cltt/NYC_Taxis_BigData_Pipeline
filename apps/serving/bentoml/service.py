import math
import numpy as np
import pandas as pd
import bentoml
from bentoml.io import JSON

from schemas import TripRequest, PredictionResponse

MODEL_NAME = "XGB_NYC_Fare"

# Create a runner from the locally stored BentoML model
nyc_taxi_runner = bentoml.mlflow.get(MODEL_NAME).to_runner()

# Define the service
svc = bentoml.Service("nyc_taxi_fare_service", runners=[nyc_taxi_runner])

FEATURE_COLS = [
    "passenger_count",
    "trip_distance",
    "trip_duration_seconds",
    "speed",
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
        "location_cluster":      (pu + do_) % 5,
        "temporal_cluster":      hour // 6,
    }

    X = pd.DataFrame([features])[FEATURE_COLS]
    return X.astype("float32")


@svc.api(input=JSON(pydantic_model=TripRequest), output=JSON(pydantic_model=PredictionResponse))
def predict(request: TripRequest) -> PredictionResponse:
    X = _build_features(request)
    X = X.replace([np.inf, -np.inf], 0).fillna(0)
    
    # Call the runner (uses adaptive batching under the hood if traffic is high)
    predicted_arr = nyc_taxi_runner.predict.run(X)
    predicted = float(predicted_arr[0])

    if predicted < 0:
        predicted = 0.0

    # Return structured Pydantic response
    return PredictionResponse(
        predicted_fare=round(predicted, 2),
        model_name=MODEL_NAME,
        model_version=nyc_taxi_runner.models[0].tag.version
    )
