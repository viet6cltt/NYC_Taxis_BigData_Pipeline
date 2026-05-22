import os

# MinIO / S3
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://minio-api.storage.svc.cluster.local:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY",  "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY",  "minioadmin")

# MLflow's S3 artifact repository uses boto3, which reads AWS_* credentials.
os.environ.setdefault("MLFLOW_S3_ENDPOINT_URL", MINIO_ENDPOINT)
os.environ.setdefault("AWS_ACCESS_KEY_ID", MINIO_ACCESS_KEY)
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", MINIO_SECRET_KEY)

# Input / lookup / output
SILVER_STARTED_PATH = os.getenv(
    "SILVER_STARTED_PATH",
    "s3a://lakehouse/silver/nyc-taxi/trip_started",
)
GOLD_ROUTE_ESTIMATES_PATH = os.getenv(
    "GOLD_ROUTE_ESTIMATES_PATH",
    "s3a://lakehouse/gold/ml/route_estimates",
)
GOLD_PREDICTIONS_PATH = os.getenv(
    "GOLD_PREDICTIONS_PATH",
    "s3a://lakehouse/gold/ml/predictions",
)
CHECKPOINT_LOCATION = os.getenv(
    "CHECKPOINT_LOCATION",
    "s3a://lakehouse/_checkpoints/gold/ml/stream_predict",
)
STARTING_VERSION = os.getenv("STARTING_VERSION", "").strip()

# MLflow
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow.mlflow.svc.cluster.local:5000")
MODEL_NAME          = os.getenv("MODEL_NAME",           "XGB_NYC_Fare")
MODEL_STAGE         = os.getenv("MODEL_STAGE",          "Production")

# Streaming
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "30 seconds")
MAX_FILES_PER_TRIGGER = os.getenv("MAX_FILES_PER_TRIGGER", "8").strip()
PREDICTION_LOG_SAMPLE_ROWS = max(0, int(os.getenv("PREDICTION_LOG_SAMPLE_ROWS", "10")))

# Feature helper settings. Keep defaults aligned with feature_engineering.
N_LOCATION_CLUSTERS = int(os.getenv("N_LOCATION_CLUSTERS", "5"))
N_TEMPORAL_CLUSTERS = int(os.getenv("N_TEMPORAL_CLUSTERS", "4"))
MIN_TRIP_DISTANCE = float(os.getenv("MIN_TRIP_DISTANCE", "0.05"))
MAX_TRIP_DISTANCE = float(os.getenv("MAX_TRIP_DISTANCE", "100.0"))
MIN_TRIP_DURATION_SECONDS = int(os.getenv("MIN_TRIP_DURATION_SECONDS", "60"))
MAX_TRIP_DURATION_SECONDS = int(os.getenv("MAX_TRIP_DURATION_SECONDS", "14400"))
MIN_AVG_SPEED_MPH = float(os.getenv("MIN_AVG_SPEED_MPH", "1.0"))
MAX_AVG_SPEED_MPH = float(os.getenv("MAX_AVG_SPEED_MPH", "80.0"))
MAX_PASSENGER_COUNT = int(os.getenv("MAX_PASSENGER_COUNT", "6"))

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
