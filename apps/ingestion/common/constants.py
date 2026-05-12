EVENT_TYPE_TRIP_STARTED = "trip_started"
EVENT_TYPE_TRIP_COMPLETED = "trip_completed"

# Keep EVENT_TYPE as a compatibility alias for batch/completed-trip ingestion.
EVENT_TYPE = EVENT_TYPE_TRIP_COMPLETED
SCHEMA_VERSION = "2.0"

EVENT_KIND_STARTED = "started"
EVENT_KIND_COMPLETED = "completed"

KAFKA_TOPIC_TRIP_STARTED = "nyc-taxi-trip-started"
KAFKA_TOPIC_TRIP_COMPLETED = "nyc-taxi-trip-completed"

PICKUP_DATETIME_FIELD = "tpep_pickup_datetime"
DROPOFF_DATETIME_FIELD = "tpep_dropoff_datetime"

INGEST_MODE_BATCH = "batch"
INGEST_MODE_STREAMING = "streaming"
