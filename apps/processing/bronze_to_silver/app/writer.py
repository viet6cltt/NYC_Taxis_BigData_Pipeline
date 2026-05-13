import time

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType

from app.config import (
    CHECKPOINT_LOCATION,
    LIFECYCLE_MERGE_SINCE_TIMESTAMP,
    LIFECYCLE_MERGE_UNTIL_TIMESTAMP,
    LIFECYCLE_PATH,
    LIFECYCLE_TTL_HOURS,
    OUTPUT_PATH,
    SILVER_COMPLETED_PATH,
    SILVER_STARTED_PATH,
    TRIGGER_INTERVAL,
)
from app.transform import (
    LIFECYCLE_COLUMNS,
    LIFECYCLE_TYPES,
    lifecycle_from_completed,
    lifecycle_from_started,
)


def delta_table_exists(spark: SparkSession, path: str) -> bool:
    try:
        return bool(DeltaTable.isDeltaTable(spark, path))
    except Exception:
        return False


def lifecycle_schema() -> StructType:
    return StructType([
        StructField(name, LIFECYCLE_TYPES[name], True)
        for name in LIFECYCLE_COLUMNS
    ])


def ensure_lifecycle_table(spark: SparkSession) -> None:
    if delta_table_exists(spark, LIFECYCLE_PATH):
        return

    schema = lifecycle_schema()
    for attempt in range(1, 4):
        try:
            (
                spark.createDataFrame([], schema)
                .write
                .format("delta")
                .mode("ignore")
                .partitionBy("year_month")
                .save(LIFECYCLE_PATH)
            )
            return
        except Exception:
            if delta_table_exists(spark, LIFECYCLE_PATH):
                return
            if attempt == 3:
                raise
            time.sleep(2 * attempt)


def _is_empty(df: DataFrame) -> bool:
    return df.limit(1).count() == 0


def _run_with_retry(operation) -> None:
    for attempt in range(1, 4):
        try:
            operation()
            return
        except Exception:
            if attempt == 3:
                raise
            time.sleep(2 * attempt)


def _merge_started(spark: SparkSession, updates_df: DataFrame) -> None:
    target = DeltaTable.forPath(spark, LIFECYCLE_PATH)
    (
        target.alias("target")
        .merge(
            updates_df.alias("source"),
            "target.year_month = source.year_month AND target.trip_id = source.trip_id",
        )
        .whenMatchedUpdate(
            condition=(
                "target.started_event_time IS NULL "
                "OR source.started_event_time >= target.started_event_time"
            ),
            set={
                "status": (
                    "CASE WHEN target.status = 'completed' "
                    "THEN 'completed' ELSE source.status END"
                ),
                "started_event_id": "source.started_event_id",
                "started_event_time": "source.started_event_time",
                "started_ingest_timestamp": "source.started_ingest_timestamp",
                "trip_date": "coalesce(source.trip_date, target.trip_date)",
                "trip_hour": "coalesce(source.trip_hour, target.trip_hour)",
                "year_month": "coalesce(source.year_month, target.year_month)",
                "vendor_id": "coalesce(source.vendor_id, target.vendor_id)",
                "pickup_datetime": "coalesce(source.pickup_datetime, target.pickup_datetime)",
                "passenger_count": "coalesce(source.passenger_count, target.passenger_count)",
                "pulocation_id": "coalesce(source.pulocation_id, target.pulocation_id)",
                "dolocation_id": "coalesce(source.dolocation_id, target.dolocation_id)",
                "updated_at": "source.updated_at",
            },
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def _merge_completed(spark: SparkSession, updates_df: DataFrame) -> None:
    target = DeltaTable.forPath(spark, LIFECYCLE_PATH)
    (
        target.alias("target")
        .merge(
            updates_df.alias("source"),
            "target.year_month = source.year_month AND target.trip_id = source.trip_id",
        )
        .whenMatchedUpdate(
            condition=(
                "target.completed_event_time IS NULL "
                "OR source.completed_event_time >= target.completed_event_time"
            ),
            set={
                "status": "'completed'",
                "completed_event_id": "source.completed_event_id",
                "completed_event_time": "source.completed_event_time",
                "completed_ingest_timestamp": "source.completed_ingest_timestamp",
                "trip_date": "coalesce(target.trip_date, source.trip_date)",
                "trip_hour": "coalesce(target.trip_hour, source.trip_hour)",
                "year_month": "coalesce(target.year_month, source.year_month)",
                "vendor_id": "coalesce(target.vendor_id, source.vendor_id)",
                "pickup_datetime": "coalesce(target.pickup_datetime, source.pickup_datetime)",
                "dropoff_datetime": "source.dropoff_datetime",
                "passenger_count": "coalesce(target.passenger_count, source.passenger_count)",
                "trip_distance": "source.trip_distance",
                "trip_duration_seconds": "source.trip_duration_seconds",
                "rate_code_id": "source.rate_code_id",
                "store_and_fwd_flag": "source.store_and_fwd_flag",
                "pulocation_id": "coalesce(target.pulocation_id, source.pulocation_id)",
                "dolocation_id": "coalesce(target.dolocation_id, source.dolocation_id)",
                "payment_type": "source.payment_type",
                "payment_type_desc": "source.payment_type_desc",
                "fare_amount": "source.fare_amount",
                "extra": "source.extra",
                "mta_tax": "source.mta_tax",
                "tip_amount": "source.tip_amount",
                "tolls_amount": "source.tolls_amount",
                "improvement_surcharge": "source.improvement_surcharge",
                "total_amount": "source.total_amount",
                "congestion_surcharge": "source.congestion_surcharge",
                "airport_fee": "source.airport_fee",
                "updated_at": "source.updated_at",
            },
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def merge_lifecycle(clean_df: DataFrame, silver_job: str, batch_id: int) -> None:
    spark = clean_df.sparkSession
    ensure_lifecycle_table(spark)

    if silver_job == "started":
        updates_df = lifecycle_from_started(clean_df)
    elif silver_job == "completed":
        updates_df = lifecycle_from_completed(clean_df)
    else:
        raise ValueError(f"Unsupported lifecycle merge job: {silver_job}")

    if _is_empty(updates_df):
        return

    if silver_job == "started":
        _run_with_retry(lambda: _merge_started(spark, updates_df))
    else:
        _run_with_retry(lambda: _merge_completed(spark, updates_df))


def append_clean_table(clean_df: DataFrame) -> None:
    if _is_empty(clean_df):
        return

    (
        clean_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("year_month")
        .save(OUTPUT_PATH)
    )


def process_clean_and_lifecycle_batch(clean_df: DataFrame, batch_id: int, silver_job: str) -> None:
    if _is_empty(clean_df):
        return
    append_clean_table(clean_df)
    print(
        "[bronze_to_silver] Clean table appended; lifecycle MERGE is handled "
        f"by the separate lifecycle job. job={silver_job}, batch_id={batch_id}"
    )


def write_clean_and_lifecycle_batch(clean_df: DataFrame, silver_job: str) -> None:
    process_clean_and_lifecycle_batch(clean_df, 0, silver_job)


def write_clean_and_lifecycle_streaming(clean_df: DataFrame, silver_job: str) -> None:
    query = (
        clean_df.writeStream
        .foreachBatch(lambda batch_df, batch_id: process_clean_and_lifecycle_batch(
            batch_df,
            batch_id,
            silver_job,
        ))
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    query.awaitTermination()


def _read_optional_delta_table(spark: SparkSession, path: str) -> DataFrame | None:
    if not delta_table_exists(spark, path):
        print(f"[bronze_to_silver] Skip missing Delta source: {path}")
        return None
    return spark.read.format("delta").load(path)


def _filter_lifecycle_source(df: DataFrame) -> DataFrame:
    if LIFECYCLE_MERGE_SINCE_TIMESTAMP:
        df = df.filter(
            F.col("ingest_timestamp") >
            F.to_timestamp(F.lit(LIFECYCLE_MERGE_SINCE_TIMESTAMP))
        )
    if LIFECYCLE_MERGE_UNTIL_TIMESTAMP:
        df = df.filter(
            F.col("ingest_timestamp") <=
            F.to_timestamp(F.lit(LIFECYCLE_MERGE_UNTIL_TIMESTAMP))
        )
    return df


def merge_lifecycle_from_silver(spark: SparkSession) -> None:
    ensure_lifecycle_table(spark)

    started_df = _read_optional_delta_table(spark, SILVER_STARTED_PATH)
    if started_df is not None:
        started_df = _filter_lifecycle_source(started_df)
        merge_lifecycle(started_df, "started", 0)

    completed_df = _read_optional_delta_table(spark, SILVER_COMPLETED_PATH)
    if completed_df is not None:
        completed_df = _filter_lifecycle_source(completed_df)
        merge_lifecycle(completed_df, "completed", 0)


def expire_lifecycle(spark: SparkSession) -> None:
    if not delta_table_exists(spark, LIFECYCLE_PATH):
        print(f"[bronze_to_silver] Lifecycle table does not exist: {LIFECYCLE_PATH}")
        return

    target = DeltaTable.forPath(spark, LIFECYCLE_PATH)
    _run_with_retry(
        lambda: target.update(
            condition=(
                "status = 'started' "
                f"AND started_event_time < current_timestamp() - INTERVAL {LIFECYCLE_TTL_HOURS} HOURS"
            ),
            set={
                "status": "'expired'",
                "updated_at": "current_timestamp()",
            },
        )
    )
