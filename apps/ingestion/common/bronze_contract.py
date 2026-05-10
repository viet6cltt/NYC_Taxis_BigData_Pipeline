from pyspark.sql.types import (
    LongType, StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)
from typing import List

BRONZE_METADATA_COLUMNS = [
    "event_id",
    "event_type",
    "schema_version",
    "trip_id",
    "source_file",
    "ingest_mode",
    "ingest_timestamp",
    "event_time",
    "trip_date",
]

BRONZE_STARTED_COLUMNS = BRONZE_METADATA_COLUMNS + [
    "vendor_id",
    "pickup_datetime",
    "passenger_count",
    "pulocation_id",
    "dolocation_id",
]

BRONZE_COMPLETED_COLUMNS = BRONZE_METADATA_COLUMNS + [
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "trip_duration_seconds",
    "rate_code_id",
    "store_and_fwd_flag",
    "pulocation_id",
    "dolocation_id",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
]

BRONZE_STARTED_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("schema_version", StringType(), False),
    StructField("trip_id", StringType(), False),
    StructField("source_file", StringType(), True),
    StructField("ingest_mode", StringType(), False),
    StructField("ingest_timestamp", TimestampType(), False),
    StructField("event_time", TimestampType(), True),
    StructField("trip_date", StringType(), True),
    StructField("vendor_id", IntegerType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("passenger_count", LongType(), True),
    StructField("pulocation_id", IntegerType(), True),
    StructField("dolocation_id", IntegerType(), True),
])

BRONZE_COMPLETED_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("schema_version", StringType(), False),
    StructField("trip_id", StringType(), False),
    StructField("source_file", StringType(), True),
    StructField("ingest_mode", StringType(), False),
    StructField("ingest_timestamp", TimestampType(), False),
    StructField("event_time", TimestampType(), True),
    StructField("trip_date", StringType(), True),
    StructField("vendor_id", IntegerType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", LongType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("trip_duration_seconds", LongType(), True),
    StructField("rate_code_id", LongType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("pulocation_id", IntegerType(), True),
    StructField("dolocation_id", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("airport_fee", DoubleType(), True),
])

# Compatibility aliases for the current batch/completed-trip path.
BRONZE_COLUMNS = BRONZE_COMPLETED_COLUMNS
BRONZE_SCHEMA = BRONZE_COMPLETED_SCHEMA


def normalize_event_kind(event_kind: str) -> str:
    normalized = (event_kind or "").lower().replace("trip_", "")
    if normalized not in {"started", "completed"}:
        raise ValueError("event_kind must be one of: started, completed")
    return normalized


def bronze_columns_for_event_kind(event_kind: str) -> List[str]:
    normalized = normalize_event_kind(event_kind)
    if normalized == "started":
        return BRONZE_STARTED_COLUMNS
    return BRONZE_COMPLETED_COLUMNS


def bronze_schema_for_event_kind(event_kind: str) -> StructType:
    normalized = normalize_event_kind(event_kind)
    if normalized == "started":
        return BRONZE_STARTED_SCHEMA
    return BRONZE_COMPLETED_SCHEMA
