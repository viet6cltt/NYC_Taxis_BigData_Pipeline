import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructField, StructType

from app.config import (
    CHECKPOINT_LOCATION,
    LIFECYCLE_PATH,
    LIFECYCLE_TTL_HOURS,
    OUTPUT_PATH,
    TRIGGER_INTERVAL,
)
from app.transform import (
    LIFECYCLE_COLUMNS,
    LIFECYCLE_TYPES,
    lifecycle_from_completed,
    lifecycle_from_started,
)


def _delta_log_path(path: str) -> str:
    return path.rstrip("/") + "/_delta_log"


def delta_table_exists(spark: SparkSession, path: str) -> bool:
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    delta_log_path = jvm.org.apache.hadoop.fs.Path(_delta_log_path(path))
    fs = delta_log_path.getFileSystem(hadoop_conf)
    return bool(fs.exists(delta_log_path))


def lifecycle_schema() -> StructType:
    return StructType([
        StructField(name, LIFECYCLE_TYPES[name], True)
        for name in LIFECYCLE_COLUMNS
    ])


def ensure_lifecycle_table(spark: SparkSession) -> None:
    if delta_table_exists(spark, LIFECYCLE_PATH):
        return

    empty_df = spark.createDataFrame([], lifecycle_schema())
    for attempt in range(1, 4):
        try:
            (
                empty_df.write
                .format("delta")
                .mode("ignore")
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


def _run_sql_with_retry(spark: SparkSession, sql_text: str) -> None:
    for attempt in range(1, 4):
        try:
            spark.sql(sql_text)
            return
        except Exception:
            if attempt == 3:
                raise
            time.sleep(2 * attempt)


def _merge_started_sql(view_name: str) -> str:
    return f"""
    MERGE INTO delta.`{LIFECYCLE_PATH}` AS target
    USING {view_name} AS source
    ON target.trip_id = source.trip_id
    WHEN MATCHED AND (
      target.started_event_time IS NULL
      OR source.started_event_time >= target.started_event_time
    ) THEN UPDATE SET
      target.status = CASE
        WHEN target.status = 'completed' THEN 'completed'
        ELSE source.status
      END,
      target.started_event_id = source.started_event_id,
      target.started_event_time = source.started_event_time,
      target.started_ingest_timestamp = source.started_ingest_timestamp,
      target.trip_date = COALESCE(source.trip_date, target.trip_date),
      target.trip_hour = COALESCE(source.trip_hour, target.trip_hour),
      target.year_month = COALESCE(source.year_month, target.year_month),
      target.vendor_id = COALESCE(source.vendor_id, target.vendor_id),
      target.pickup_datetime = COALESCE(source.pickup_datetime, target.pickup_datetime),
      target.passenger_count = COALESCE(source.passenger_count, target.passenger_count),
      target.pulocation_id = COALESCE(source.pulocation_id, target.pulocation_id),
      target.dolocation_id = COALESCE(source.dolocation_id, target.dolocation_id),
      target.updated_at = source.updated_at
    WHEN NOT MATCHED THEN INSERT *
    """


def _merge_completed_sql(view_name: str) -> str:
    return f"""
    MERGE INTO delta.`{LIFECYCLE_PATH}` AS target
    USING {view_name} AS source
    ON target.trip_id = source.trip_id
    WHEN MATCHED AND (
      target.completed_event_time IS NULL
      OR source.completed_event_time >= target.completed_event_time
    ) THEN UPDATE SET
      target.status = 'completed',
      target.completed_event_id = source.completed_event_id,
      target.completed_event_time = source.completed_event_time,
      target.completed_ingest_timestamp = source.completed_ingest_timestamp,
      target.trip_date = COALESCE(target.trip_date, source.trip_date),
      target.trip_hour = COALESCE(target.trip_hour, source.trip_hour),
      target.year_month = COALESCE(target.year_month, source.year_month),
      target.vendor_id = COALESCE(target.vendor_id, source.vendor_id),
      target.pickup_datetime = COALESCE(target.pickup_datetime, source.pickup_datetime),
      target.dropoff_datetime = source.dropoff_datetime,
      target.passenger_count = COALESCE(target.passenger_count, source.passenger_count),
      target.trip_distance = source.trip_distance,
      target.trip_duration_seconds = source.trip_duration_seconds,
      target.rate_code_id = source.rate_code_id,
      target.store_and_fwd_flag = source.store_and_fwd_flag,
      target.pulocation_id = COALESCE(target.pulocation_id, source.pulocation_id),
      target.dolocation_id = COALESCE(target.dolocation_id, source.dolocation_id),
      target.payment_type = source.payment_type,
      target.payment_type_desc = source.payment_type_desc,
      target.fare_amount = source.fare_amount,
      target.extra = source.extra,
      target.mta_tax = source.mta_tax,
      target.tip_amount = source.tip_amount,
      target.tolls_amount = source.tolls_amount,
      target.improvement_surcharge = source.improvement_surcharge,
      target.total_amount = source.total_amount,
      target.congestion_surcharge = source.congestion_surcharge,
      target.airport_fee = source.airport_fee,
      target.updated_at = source.updated_at
    WHEN NOT MATCHED THEN INSERT *
    """


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

    view_name = f"lifecycle_updates_{silver_job}_{batch_id}"
    updates_df.createOrReplaceTempView(view_name)
    sql_text = (
        _merge_started_sql(view_name)
        if silver_job == "started"
        else _merge_completed_sql(view_name)
    )
    _run_sql_with_retry(spark, sql_text)


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
    merge_lifecycle(clean_df, silver_job, batch_id)
    append_clean_table(clean_df)


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


def expire_lifecycle(spark: SparkSession) -> None:
    if not delta_table_exists(spark, LIFECYCLE_PATH):
        print(f"[bronze_to_silver] Lifecycle table does not exist: {LIFECYCLE_PATH}")
        return

    sql_text = f"""
    UPDATE delta.`{LIFECYCLE_PATH}`
    SET
      status = 'expired',
      updated_at = current_timestamp()
    WHERE status = 'started'
      AND started_event_time < current_timestamp() - INTERVAL {LIFECYCLE_TTL_HOURS} HOURS
    """
    _run_sql_with_retry(spark, sql_text)
