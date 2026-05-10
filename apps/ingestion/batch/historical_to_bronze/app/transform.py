from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.bronze_mapper import map_to_bronze
from common.bronze_contract import BRONZE_COLUMNS
from common.constants import (
    EVENT_TYPE,
    SCHEMA_VERSION,
    INGEST_MODE_BATCH
)

# Input is the historical data in the raw format, we need to transform it to the bronze format
def transform(df: DataFrame) -> DataFrame:
    mapper = map_to_bronze(df)
    trip_fingerprint = F.concat_ws(
        "||",
        F.input_file_name(),
        F.col("vendor_id").cast("string"),
        F.col("pickup_datetime").cast("string"),
        F.col("dropoff_datetime").cast("string"),
        F.col("pulocation_id").cast("string"),
        F.col("dolocation_id").cast("string"),
        F.col("fare_amount").cast("string"),
        F.col("total_amount").cast("string"),
    )
    mapper = mapper.withColumn("trip_id", F.sha2(trip_fingerprint, 256))
    mapper = mapper.withColumn(
        "event_id",
        F.sha2(
            F.concat_ws(
                "||",
                F.lit(SCHEMA_VERSION),
                F.lit(EVENT_TYPE),
                F.col("trip_id"),
            ),
            256,
        ),
    )
    # Add the metadata columns
    return mapper.select(
        F.col("event_id"),
        F.lit(EVENT_TYPE).alias("event_type"),
        F.lit(SCHEMA_VERSION).alias("schema_version"),
        F.col("trip_id"),
        F.input_file_name().alias("source_file"),
        F.lit(INGEST_MODE_BATCH).alias("ingest_mode"),
        F.current_timestamp().alias("ingest_timestamp"),
        F.col("dropoff_datetime").cast("timestamp").alias("event_time"),
        F.date_format(F.col("pickup_datetime").cast("timestamp"), "yyyy-MM-dd").alias("trip_date"),
        *[c for c in BRONZE_COLUMNS if c not in [
            "event_id", "event_type", "schema_version", "trip_id", "source_file",
            "ingest_mode", "ingest_timestamp", "event_time", "trip_date"
        ]]
    ).select(*BRONZE_COLUMNS)
