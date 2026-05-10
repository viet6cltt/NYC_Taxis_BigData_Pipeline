from pyspark.sql import DataFrame
from pyspark.sql.column import Column
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

from app.config import WATERMARK_DELAY


STARTED_EVENT_TYPE = "trip_started"
COMPLETED_EVENT_TYPE = "trip_completed"

STARTED_COLUMNS = [
    "event_id",
    "event_type",
    "schema_version",
    "trip_id",
    "source_file",
    "ingest_mode",
    "ingest_timestamp",
    "event_time",
    "trip_date",
    "trip_hour",
    "year_month",
    "vendor_id",
    "pickup_datetime",
    "passenger_count",
    "pulocation_id",
    "dolocation_id",
]

COMPLETED_COLUMNS = [
    "event_id",
    "event_type",
    "schema_version",
    "trip_id",
    "source_file",
    "ingest_mode",
    "ingest_timestamp",
    "event_time",
    "trip_date",
    "trip_hour",
    "year_month",
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
    "payment_type_desc",
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

LIFECYCLE_COLUMNS = [
    "trip_id",
    "status",
    "started_event_id",
    "completed_event_id",
    "started_event_time",
    "completed_event_time",
    "started_ingest_timestamp",
    "completed_ingest_timestamp",
    "trip_date",
    "trip_hour",
    "year_month",
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
    "payment_type_desc",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
    "created_at",
    "updated_at",
]

LIFECYCLE_TYPES = {
    "trip_id": StringType(),
    "status": StringType(),
    "started_event_id": StringType(),
    "completed_event_id": StringType(),
    "started_event_time": TimestampType(),
    "completed_event_time": TimestampType(),
    "started_ingest_timestamp": TimestampType(),
    "completed_ingest_timestamp": TimestampType(),
    "trip_date": StringType(),
    "trip_hour": IntegerType(),
    "year_month": StringType(),
    "vendor_id": IntegerType(),
    "pickup_datetime": TimestampType(),
    "dropoff_datetime": TimestampType(),
    "passenger_count": LongType(),
    "trip_distance": DoubleType(),
    "trip_duration_seconds": LongType(),
    "rate_code_id": LongType(),
    "store_and_fwd_flag": StringType(),
    "pulocation_id": IntegerType(),
    "dolocation_id": IntegerType(),
    "payment_type": IntegerType(),
    "payment_type_desc": StringType(),
    "fare_amount": DoubleType(),
    "extra": DoubleType(),
    "mta_tax": DoubleType(),
    "tip_amount": DoubleType(),
    "tolls_amount": DoubleType(),
    "improvement_surcharge": DoubleType(),
    "total_amount": DoubleType(),
    "congestion_surcharge": DoubleType(),
    "airport_fee": DoubleType(),
    "created_at": TimestampType(),
    "updated_at": TimestampType(),
}


def add_payment_type_desc(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "payment_type_desc",
        F.when(F.col("payment_type") == 1, "credit_card")
        .when(F.col("payment_type") == 2, "cash")
        .when(F.col("payment_type") == 3, "no_charge")
        .when(F.col("payment_type") == 4, "dispute")
        .when(F.col("payment_type") == 5, "unknown")
        .when(F.col("payment_type") == 6, "voided_trip")
        .otherwise("other"),
    )


def add_temporal_columns(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("trip_hour", F.hour(F.col("pickup_datetime")))
        .withColumn("year_month", F.date_format(F.col("pickup_datetime"), "yyyy-MM"))
    )


def dedup_events(df: DataFrame, pipeline_mode: str) -> DataFrame:
    if pipeline_mode == "streaming":
        return df.withWatermark("event_time", WATERMARK_DELAY).dropDuplicates(["event_id"])
    return df.dropDuplicates(["event_id"])


def _valid_passenger_count() -> Column:
    return (
        F.col("passenger_count").isNull()
        | ((F.col("passenger_count") > 0) & (F.col("passenger_count") < 10))
    )


def transform_started(bronze_df: DataFrame, pipeline_mode: str) -> DataFrame:
    df = (
        bronze_df
        .filter(F.col("event_type") == STARTED_EVENT_TYPE)
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("trip_id").isNotNull())
        .filter(F.col("pickup_datetime").isNotNull())
        .filter(F.col("pulocation_id").isNotNull())
        .filter(F.col("dolocation_id").isNotNull())
        .filter(_valid_passenger_count())
    )
    df = add_temporal_columns(dedup_events(df, pipeline_mode))
    return df.select(*STARTED_COLUMNS)


def transform_completed(bronze_df: DataFrame, pipeline_mode: str) -> DataFrame:
    df = (
        bronze_df
        .filter(F.col("event_type") == COMPLETED_EVENT_TYPE)
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("trip_id").isNotNull())
        .filter(F.col("pickup_datetime").isNotNull())
        .filter(F.col("dropoff_datetime").isNotNull())
        .filter(F.col("dropoff_datetime") > F.col("pickup_datetime"))
        .filter(F.col("trip_distance").isNotNull() & (F.col("trip_distance") > 0))
        .filter(F.col("trip_distance") < 1000)
        .filter(F.col("fare_amount").isNotNull() & (F.col("fare_amount") >= 0))
        .filter(F.col("total_amount").isNotNull() & (F.col("total_amount") >= 0))
        .filter(_valid_passenger_count())
    )

    computed_duration = (
        F.unix_timestamp(F.col("dropoff_datetime")) -
        F.unix_timestamp(F.col("pickup_datetime"))
    )
    if "trip_duration_seconds" in df.columns:
        df = df.withColumn(
            "trip_duration_seconds",
            F.coalesce(F.col("trip_duration_seconds"), computed_duration),
        )
    else:
        df = df.withColumn("trip_duration_seconds", computed_duration)

    df = (
        df
        .filter(F.col("trip_duration_seconds").isNotNull())
        .filter(F.col("trip_duration_seconds") > 0)
    )
    df = add_temporal_columns(add_payment_type_desc(dedup_events(df, pipeline_mode)))
    return df.select(*COMPLETED_COLUMNS)


def _null_col(name: str) -> Column:
    return F.lit(None).cast(LIFECYCLE_TYPES[name]).alias(name)


def lifecycle_from_started(started_df: DataFrame) -> DataFrame:
    df = started_df.dropDuplicates(["trip_id"])
    now = F.current_timestamp()
    return df.select(
        F.col("trip_id"),
        F.lit("started").alias("status"),
        F.col("event_id").alias("started_event_id"),
        _null_col("completed_event_id"),
        F.col("event_time").alias("started_event_time"),
        _null_col("completed_event_time"),
        F.col("ingest_timestamp").alias("started_ingest_timestamp"),
        _null_col("completed_ingest_timestamp"),
        F.col("trip_date"),
        F.col("trip_hour"),
        F.col("year_month"),
        F.col("vendor_id"),
        F.col("pickup_datetime"),
        _null_col("dropoff_datetime"),
        F.col("passenger_count"),
        _null_col("trip_distance"),
        _null_col("trip_duration_seconds"),
        _null_col("rate_code_id"),
        _null_col("store_and_fwd_flag"),
        F.col("pulocation_id"),
        F.col("dolocation_id"),
        _null_col("payment_type"),
        _null_col("payment_type_desc"),
        _null_col("fare_amount"),
        _null_col("extra"),
        _null_col("mta_tax"),
        _null_col("tip_amount"),
        _null_col("tolls_amount"),
        _null_col("improvement_surcharge"),
        _null_col("total_amount"),
        _null_col("congestion_surcharge"),
        _null_col("airport_fee"),
        now.alias("created_at"),
        now.alias("updated_at"),
    ).select(*LIFECYCLE_COLUMNS)


def lifecycle_from_completed(completed_df: DataFrame) -> DataFrame:
    df = completed_df.dropDuplicates(["trip_id"])
    now = F.current_timestamp()
    return df.select(
        F.col("trip_id"),
        F.lit("completed").alias("status"),
        _null_col("started_event_id"),
        F.col("event_id").alias("completed_event_id"),
        _null_col("started_event_time"),
        F.col("event_time").alias("completed_event_time"),
        _null_col("started_ingest_timestamp"),
        F.col("ingest_timestamp").alias("completed_ingest_timestamp"),
        F.col("trip_date"),
        F.col("trip_hour"),
        F.col("year_month"),
        F.col("vendor_id"),
        F.col("pickup_datetime"),
        F.col("dropoff_datetime"),
        F.col("passenger_count"),
        F.col("trip_distance"),
        F.col("trip_duration_seconds"),
        F.col("rate_code_id"),
        F.col("store_and_fwd_flag"),
        F.col("pulocation_id"),
        F.col("dolocation_id"),
        F.col("payment_type"),
        F.col("payment_type_desc"),
        F.col("fare_amount"),
        F.col("extra"),
        F.col("mta_tax"),
        F.col("tip_amount"),
        F.col("tolls_amount"),
        F.col("improvement_surcharge"),
        F.col("total_amount"),
        F.col("congestion_surcharge"),
        F.col("airport_fee"),
        now.alias("created_at"),
        now.alias("updated_at"),
    ).select(*LIFECYCLE_COLUMNS)


def transform(bronze_df: DataFrame, pipeline_mode: str, silver_job: str) -> DataFrame:
    if silver_job == "started":
        return transform_started(bronze_df, pipeline_mode)
    if silver_job == "completed":
        return transform_completed(bronze_df, pipeline_mode)
    raise ValueError(f"Unsupported transform job: {silver_job}")
