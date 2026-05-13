import os 
from common.bronze_contract import normalize_event_kind
from common.constants import KAFKA_TOPIC_TRIP_COMPLETED, KAFKA_TOPIC_TRIP_STARTED

EVENT_KIND = normalize_event_kind(os.getenv("EVENT_KIND", "started"))

DEFAULT_TOPIC_BY_KIND = {
    "started": KAFKA_TOPIC_TRIP_STARTED,
    "completed": KAFKA_TOPIC_TRIP_COMPLETED,
}
DEFAULT_OUTPUT_PATH_BY_KIND = {
    "started": "s3a://lakehouse/bronze/nyc-taxi/trip_started",
    "completed": "s3a://lakehouse/bronze/nyc-taxi/trip_completed",
}
DEFAULT_CHECKPOINT_BY_KIND = {
    "started": "s3a://lakehouse/_checkpoints/bronze/trip_started/kafka_to_bronze",
    "completed": "s3a://lakehouse/_checkpoints/bronze/trip_completed/kafka_to_bronze",
}

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC_BY_KIND[EVENT_KIND])
OUTPUT_PATH = os.getenv("OUTPUT_PATH", DEFAULT_OUTPUT_PATH_BY_KIND[EVENT_KIND])
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", DEFAULT_CHECKPOINT_BY_KIND[EVENT_KIND])
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "my-kafka-cluster-kafka-bootstrap.ingestion.svc.cluster.local:9092")
AVRO_SCHEMA_PATH = os.getenv("AVRO_SCHEMA_PATH", "schemas/taxi_trip_event.avsc")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "30 seconds")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio-api.storage.svc.cluster.local:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
