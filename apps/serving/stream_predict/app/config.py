import os

# MinIO / S3
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://minio-api.minio.svc.cluster.local:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY",  "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY",  "minioadmin")

# MLflow's S3 artifact repository uses boto3, which reads AWS_* credentials.
os.environ.setdefault("MLFLOW_S3_ENDPOINT_URL", MINIO_ENDPOINT)
os.environ.setdefault("AWS_ACCESS_KEY_ID", MINIO_ACCESS_KEY)
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", MINIO_SECRET_KEY)

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-cluster-kafka-bootstrap.ingestion.svc.cluster.local:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC",              "nyc-taxi-trips")

# Output
GOLD_PREDICTIONS_PATH = os.getenv("GOLD_PREDICTIONS_PATH", "s3a://gold/predictions/")
CHECKPOINT_LOCATION   = os.getenv("CHECKPOINT_LOCATION",   "s3a://gold/checkpoints/stream_predict/")

# MLflow
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow.mlflow.svc.cluster.local:5000")
MODEL_NAME          = os.getenv("MODEL_NAME",           "XGB_NYC_Fare")
MODEL_STAGE         = os.getenv("MODEL_STAGE",          "Production")

# Streaming
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "30 seconds")

# Features (must match training)
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
