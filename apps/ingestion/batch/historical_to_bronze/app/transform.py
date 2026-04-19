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
    # Add the metadata columns
    return mapper.select(
        F.expr("uuid()").alias("event_id"),
        F.lit(EVENT_TYPE).alias("event_type"),
        F.lit(SCHEMA_VERSION).alias("schema_version"),
        F.input_file_name().alias("source_file"),
        F.lit(INGEST_MODE_BATCH).alias("ingest_mode"),
        F.current_timestamp().alias("ingest_timestamp"),
        F.col("pickup_datetime").cast("timestamp").alias("event_time"),
        F.date_format(F.col("pickup_datetime").cast("timestamp"), "yyyy-MM-dd").alias("trip_date"),
        *[c for c in BRONZE_COLUMNS if c not in [
            "event_id", "event_type", "schema_version", "source_file", 
            "ingest_mode", "ingest_timestamp", "event_time", "trip_date"
        ]]
    ).select(*BRONZE_COLUMNS)
    
    