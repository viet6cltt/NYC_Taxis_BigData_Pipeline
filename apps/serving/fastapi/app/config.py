import os

# MLflow
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow.mlflow.svc.cluster.local:5000")
MODEL_NAME          = os.getenv("MODEL_NAME",           "XGB_NYC_Fare")
MODEL_STAGE         = os.getenv("MODEL_STAGE",          "Production")

# MinIO (for reading predictions from Gold Delta Lake)
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://minio-api.storage.svc.cluster.local:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY",  "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY",  "minioadmin")

GOLD_PREDICTIONS_PATH = os.getenv("GOLD_PREDICTIONS_PATH", "s3a://lakehouse/gold/ml/predictions")

# App
APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
APP_PORT = int(os.getenv("APP_PORT", "8000"))
LOAD_MODEL_ON_STARTUP = os.getenv("LOAD_MODEL_ON_STARTUP", "true").lower() not in {"0", "false", "no"}

# Feature helper settings. Keep defaults aligned with feature_engineering.
N_LOCATION_CLUSTERS = int(os.getenv("N_LOCATION_CLUSTERS", "5"))
N_TEMPORAL_CLUSTERS = int(os.getenv("N_TEMPORAL_CLUSTERS", "4"))

# Features (must match training)
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
