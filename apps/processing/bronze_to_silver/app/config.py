import os


def _normalize_job(value: str) -> str:
    normalized = (value or "").strip().lower()
    aliases = {
        "clean_started": "started",
        "clean_completed": "completed",
        "lifecycle_started": "started",
        "lifecycle_completed": "completed",
        "lifecycle_from_started": "started",
        "lifecycle_from_completed": "completed",
        "expire_lifecycle": "expire",
    }
    normalized = aliases.get(normalized, normalized)
    if normalized not in {"started", "completed", "expire"}:
        raise ValueError("SILVER_JOB must be one of: started, completed, expire")
    return normalized


SILVER_JOB = _normalize_job(os.getenv("SILVER_JOB", "started"))
PIPELINE_MODE = os.getenv("PIPELINE_MODE", "streaming")

DEFAULT_INPUT_PATH = {
    "started": "s3a://lakehouse/bronze/nyc-taxi/trip_started",
    "completed": "s3a://lakehouse/bronze/nyc-taxi/trip_completed",
    "expire": "",
}
DEFAULT_OUTPUT_PATH = {
    "started": "s3a://lakehouse/silver/nyc-taxi/trip_started",
    "completed": "s3a://lakehouse/silver/nyc-taxi/trip_completed",
    "expire": "",
}
DEFAULT_CHECKPOINT_PATH = {
    "started": "s3a://lakehouse/_checkpoints/silver/trip_started/processor",
    "completed": "s3a://lakehouse/_checkpoints/silver/trip_completed/processor",
    "expire": "",
}

APP_NAME = f"BronzeToSilver-{SILVER_JOB}"

# INPUT_PATH/OUTPUT_PATH are the new names. BRONZE_PATH/SILVER_PATH stay as
# compatibility aliases for older submit scripts.
INPUT_PATH = os.getenv("INPUT_PATH") or os.getenv("BRONZE_PATH") or DEFAULT_INPUT_PATH[SILVER_JOB]
OUTPUT_PATH = os.getenv("OUTPUT_PATH") or os.getenv("SILVER_PATH") or DEFAULT_OUTPUT_PATH[SILVER_JOB]
CHECKPOINT_LOCATION = (
    os.getenv("CHECKPOINT_LOCATION")
    or os.getenv("SILVER_CHECKPOINT_PATH")
    or DEFAULT_CHECKPOINT_PATH[SILVER_JOB]
)
LIFECYCLE_PATH = os.getenv(
    "LIFECYCLE_PATH",
    "s3a://lakehouse/silver/nyc-taxi/trip_lifecycle",
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio-api.minio.svc.cluster.local:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "30 seconds")
WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "48 hours")
LIFECYCLE_TTL_HOURS = int(os.getenv("LIFECYCLE_TTL_HOURS", "48"))
LIFECYCLE_MERGE_ENABLED = os.getenv("LIFECYCLE_MERGE_ENABLED", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
}

STARTING_VERSION = os.getenv("STARTING_VERSION")
