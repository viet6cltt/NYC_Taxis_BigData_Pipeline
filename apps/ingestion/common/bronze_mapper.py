from pyspark.sql import DateFrame
from pyspark.sql import functions as F
from common.bronze_contract import BRONZE_COLUMNS
from common.constants import (
    PICKUP_DATETIME_FIELD,
    DROPOFF_DATETIME_FIELD
)

def map_to_bronze(df: DateFrame) -> DateFrame:
    return (
        df.select(
            F.col("VendorID").alias("vendor_id"),
            F.col(PICKUP_DATETIME_FIELD).alias("pickup_datetime"),
            F.col(DROPOFF_DATETIME_FIELD).alias("dropoff_datetime"),
            F.col("RatecodeID").alias("rate_code_id"),
            F.col("PULocationID").alias("pulocation_id"),
            F.col("DOLocationID").alias("dolocation_id"),
            F.col("Airport_fee").alias("airport_fee")
        )
    )