from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.bronze_mapper import map_to_bronze
from common.bronze_contract import BRONZE_COLUMNS 
from common.constants import (
    EVENT_TYPE,
    SCHEMA_VERSION,
    INGEST_MODE_STREAMING
)

def transform(df: DataFrame) -> DataFrame:
    return map_to_bronze(df)  