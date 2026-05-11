import os

# MinIO / S3
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio-api.minio.svc.cluster.local:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

def _normalize_job(value: str) -> str:
    normalized = (value or "").strip().lower()
    aliases = {
        "feature": "features",
        "training_features": "features",
        "ml_features": "features",
        "routes": "route_estimates",
        "route": "route_estimates",
        "actuals": "prediction_actuals",
        "quality": "model_quality_daily",
        "model_quality": "model_quality_daily",
    }
    normalized = aliases.get(normalized, normalized)
    allowed = {"route_estimates", "features", "prediction_actuals", "model_quality_daily"}
    if normalized not in allowed:
        raise ValueError(
            "GOLD_JOB must be one of: route_estimates, features, "
            "prediction_actuals, model_quality_daily"
        )
    return normalized


GOLD_JOB = _normalize_job(os.getenv("GOLD_JOB", "features"))

# Paths
SILVER_COMPLETED_PATH = (
    os.getenv("SILVER_COMPLETED_PATH")
    or os.getenv("SILVER_PATH")
    or "s3a://lakehouse/silver/nyc-taxi/trip_completed"
)
GOLD_ROUTE_ESTIMATES_PATH = os.getenv(
    "GOLD_ROUTE_ESTIMATES_PATH",
    "s3a://lakehouse/gold/ml/route_estimates",
)
GOLD_FEATURES_PATH = os.getenv(
    "GOLD_FEATURES_PATH",
    "s3a://lakehouse/gold/ml/features",
)
GOLD_PREDICTIONS_PATH = os.getenv(
    "GOLD_PREDICTIONS_PATH",
    "s3a://lakehouse/gold/ml/predictions",
)
GOLD_PREDICTION_ACTUALS_PATH = os.getenv(
    "GOLD_PREDICTION_ACTUALS_PATH",
    "s3a://lakehouse/gold/ml/prediction_actuals",
)
GOLD_MODEL_QUALITY_DAILY_PATH = os.getenv(
    "GOLD_MODEL_QUALITY_DAILY_PATH",
    "s3a://lakehouse/gold/monitoring/model_quality_daily",
)

# Lightweight deterministic cluster proxies used by training and serving.
N_LOCATION_CLUSTERS = int(os.getenv("N_LOCATION_CLUSTERS", "5"))
N_TEMPORAL_CLUSTERS = int(os.getenv("N_TEMPORAL_CLUSTERS", "4"))

# Write mode
WRITE_MODE = os.getenv("WRITE_MODE", "overwrite")  # overwrite | append
