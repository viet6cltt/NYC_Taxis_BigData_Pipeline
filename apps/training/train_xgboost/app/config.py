import os

# MinIO / S3
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT",  "http://minio-api.storage.svc.cluster.local:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

# MLflow's S3 artifact repository uses boto3, which reads AWS_* credentials.
os.environ.setdefault("MLFLOW_S3_ENDPOINT_URL", MINIO_ENDPOINT)
os.environ.setdefault("AWS_ACCESS_KEY_ID", MINIO_ACCESS_KEY)
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", MINIO_SECRET_KEY)

# Data paths
GOLD_FEATURES_PATH = os.getenv("GOLD_FEATURES_PATH", "s3a://lakehouse/gold/ml/features")

# MLflow
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow.mlops.svc.cluster.local:5000")
EXPERIMENT_NAME     = os.getenv("EXPERIMENT_NAME", "NYC_Taxi_Fare_Prediction")
MODEL_NAME          = os.getenv("MODEL_NAME", "XGB_NYC_Fare")

# Training
TEST_SIZE      = float(os.getenv("TEST_SIZE", "0.2"))
RANDOM_STATE   = int(os.getenv("RANDOM_STATE", "42"))
PROMOTE_THRESHOLD_R2 = float(os.getenv("PROMOTE_THRESHOLD_R2", "0.7"))
XGB_NUM_WORKERS = int(os.getenv("XGB_NUM_WORKERS", "2"))
SPLIT_STRATEGY = os.getenv("SPLIT_STRATEGY", "random").strip().lower()
TIME_SPLIT_MONTH = os.getenv("TIME_SPLIT_MONTH", "2024-11")

# Features used (must match Gold schema from feature_engineering/app/transform.py)
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
TARGET_COL = "fare_amount"

# XGBoost hyperparameters (matches notebook Section 11)
XGB_PARAMS = {
    "n_estimators":    int(os.getenv("XGB_N_ESTIMATORS", "100")),
    "max_depth":       int(os.getenv("XGB_MAX_DEPTH", "6")),
    "learning_rate":   float(os.getenv("XGB_LEARNING_RATE", "0.1")),
    "subsample":       float(os.getenv("XGB_SUBSAMPLE", "0.8")),
    "colsample_bytree": float(os.getenv("XGB_COLSAMPLE", "0.8")),
    "random_state":    RANDOM_STATE,
    "eval_metric":     "rmse",
    "objective":       "reg:squarederror",
}
