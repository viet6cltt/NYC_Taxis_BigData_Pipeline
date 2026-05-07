import os

# MinIO / S3
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT",  "http://minio-api.minio.svc.cluster.local:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

# Data paths
GOLD_FEATURES_PATH = os.getenv("GOLD_FEATURES_PATH", "s3a://gold/features/")

# MLflow
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow.mlflow.svc.cluster.local:5000")
EXPERIMENT_NAME     = os.getenv("EXPERIMENT_NAME", "NYC_Taxi_Fare_Prediction")
MODEL_NAME          = os.getenv("MODEL_NAME", "XGB_NYC_Fare")

# Training
TEST_SIZE      = float(os.getenv("TEST_SIZE", "0.2"))
RANDOM_STATE   = int(os.getenv("RANDOM_STATE", "42"))
PROMOTE_THRESHOLD_R2 = float(os.getenv("PROMOTE_THRESHOLD_R2", "0.7"))

# Features used (must match Gold schema from feature_engineering/app/transform.py)
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
TARGET_COL = "fare_amount"

# XGBoost hyperparameters (matches notebook Section 11)
XGB_PARAMS = {
    "n_estimators":    int(os.getenv("XGB_N_ESTIMATORS", "100")),
    "max_depth":       int(os.getenv("XGB_MAX_DEPTH", "6")),
    "learning_rate":   float(os.getenv("XGB_LEARNING_RATE", "0.1")),
    "subsample":       float(os.getenv("XGB_SUBSAMPLE", "0.8")),
    "colsample_bytree": float(os.getenv("XGB_COLSAMPLE", "0.8")),
    "random_state":    RANDOM_STATE,
    "n_jobs":          -1,
    "eval_metric":     "rmse",
}
