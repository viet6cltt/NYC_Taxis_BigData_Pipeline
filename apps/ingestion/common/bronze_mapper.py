from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from common.bronze_contract import BRONZE_COLUMNS
from common.constants import (
    PICKUP_DATETIME_FIELD,
    DROPOFF_DATETIME_FIELD
)

def map_to_bronze(df: DataFrame) -> DataFrame:
    return (
        df.withColumnRenamed("VendorID", "vendor_id")
          .withColumnRenamed(PICKUP_DATETIME_FIELD, "pickup_datetime")
          .withColumnRenamed(DROPOFF_DATETIME_FIELD, "dropoff_datetime")
          .withColumnRenamed("RatecodeID", "rate_code_id")
          .withColumnRenamed("PULocationID", "pulocation_id")
          .withColumnRenamed("DOLocationID", "dolocation_id")
          .withColumnRenamed("Airport_fee", "airport_fee")
    )