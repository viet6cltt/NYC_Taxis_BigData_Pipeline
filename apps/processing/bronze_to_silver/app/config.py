import os 

APP_NAME = "BronzeToSilver"

BRONZE_PATH = os.getenv("BRONZE_PATH", "s3a://lakehouse/bronze/nyc-taxi")
SILVER_PATH = os.getenv("SILVER_PATH", "s3a://lakehouse/silver/nyc-taxi/trips")
SILVER_CHECKPOINT_PATH = os.getenv("SILVER_CHECKPOINT_PATH", "s3a://lakehouse/silver/nyc-taxi/_checkpoint")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio-api.minio.svc.cluster.local:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "30 seconds")
WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "1 minute")

