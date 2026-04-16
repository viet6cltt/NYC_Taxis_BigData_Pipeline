import os 

TOPIC = os.getenv("KAFKA_TOPIC", "nyc-taxi-events")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "my-kafka-cluster-kafka-bootstrap.kafka:9092")
MODE = os.getenv("MODE", "historical")  # "historical" or "streaming"

DATA_DIR = os.getenv("DATA_DIR", "/data/yellow_data")
YEAR = os.getenv("YEAR", "2024")

# Historical
HISTORICAL_FLUSH_EVERY = int(os.getenv("HISTORICAL_FLUSH_EVERY", "3000"))  # Flush after every N records

# Streaming
STREAMING_SPEED_MULTIPLIER = float(os.getenv("STREAMING_SPEED_MULTIPLIER", "5.0"))  # Speed multiplier for streaming (e.g., 5.0 means 5x faster than real-time)

STREAMING_BATCH_SIZE = int(os.getenv("STREAMING_BATCH_SIZE", "100")) 
STREAMING_MAX_SLEEP_SECONDS = int(os.getenv("STREAMING_MAX_SLEEP_SECONDS", "5"))  # Max sleep time between batches in seconds
LOOP_STREAMING = os.getenv("LOOP_STREAMING", "false").lower() == "true"  # Whether to loop streaming data indefinitely