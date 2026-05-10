from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Optional
from common.bronze_contract import (
    bronze_columns_for_event_kind,
    bronze_schema_for_event_kind,
)
from common.constants import (
    PICKUP_DATETIME_FIELD,
    DROPOFF_DATETIME_FIELD
)


def _rename_raw_columns(df: DataFrame) -> DataFrame:
    return (
        df.withColumnRenamed("VendorID", "vendor_id")
          .withColumnRenamed(PICKUP_DATETIME_FIELD, "pickup_datetime")
          .withColumnRenamed(DROPOFF_DATETIME_FIELD, "dropoff_datetime")
          .withColumnRenamed("RatecodeID", "rate_code_id")
          .withColumnRenamed("PULocationID", "pulocation_id")
          .withColumnRenamed("DOLocationID", "dolocation_id")
          .withColumnRenamed("Airport_fee", "airport_fee")
    )


def _add_duration_if_possible(df: DataFrame) -> DataFrame:
    if "trip_duration_seconds" in df.columns:
        return df
    if "pickup_datetime" not in df.columns or "dropoff_datetime" not in df.columns:
        return df
    return df.withColumn(
        "trip_duration_seconds",
        F.unix_timestamp(F.col("dropoff_datetime")) -
        F.unix_timestamp(F.col("pickup_datetime"))
    )


def _select_contract_columns(df: DataFrame, event_kind: str) -> DataFrame:
    schema = bronze_schema_for_event_kind(event_kind)
    selected = df
    for field in schema.fields:
        if field.name not in selected.columns:
            selected = selected.withColumn(field.name, F.lit(None).cast(field.dataType))
        else:
            selected = selected.withColumn(field.name, F.col(field.name).cast(field.dataType))
    return selected.select(*bronze_columns_for_event_kind(event_kind))


def map_to_bronze(df: DataFrame, event_kind: Optional[str] = None) -> DataFrame:
    mapped = _add_duration_if_possible(_rename_raw_columns(df))
    if event_kind is None:
        return mapped
    return _select_contract_columns(mapped, event_kind)
