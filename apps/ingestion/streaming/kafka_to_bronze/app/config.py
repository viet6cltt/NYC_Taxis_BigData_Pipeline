import os 

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "nyc-taxi-events")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "s3a://lakehouse/bronze/nyc-taxi")  
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "s3a://lakehouse/bronze/_checkpoints/nyc_taxi_stream")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "my-kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092")


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio-api.minio.svc.cluster.local:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

