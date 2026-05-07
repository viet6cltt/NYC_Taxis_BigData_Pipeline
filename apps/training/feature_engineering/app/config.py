import os

# MinIO / S3
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio-api.minio.svc.cluster.local:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

# Paths
SILVER_PATH = os.getenv("SILVER_PATH", "s3a://silver/trips/")
GOLD_FEATURES_PATH = os.getenv("GOLD_FEATURES_PATH", "s3a://gold/features/")

# KMeans clusters (pre-defined based on notebook analysis)
N_LOCATION_CLUSTERS = int(os.getenv("N_LOCATION_CLUSTERS", "5"))
N_TEMPORAL_CLUSTERS = int(os.getenv("N_TEMPORAL_CLUSTERS", "4"))

# Write mode
WRITE_MODE = os.getenv("WRITE_MODE", "overwrite")  # overwrite | append
