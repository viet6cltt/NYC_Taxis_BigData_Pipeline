
import os

INPUT_PATH = os.getenv("STORAGE_PATH", "/data/yellow_data")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "s3a://lakehouse/bronze/nyc-taxi")  

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio-api.minio.svc.cluster.local:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

YEAR = os.getenv("YEAR", "2024")