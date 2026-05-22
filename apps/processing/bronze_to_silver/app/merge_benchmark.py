"""Lifecycle MERGE benchmark for partitioned and unpartitioned Delta targets."""

from __future__ import annotations

import json
import os
import time
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from app.spark_session import build_spark_session
from app.transform import lifecycle_from_completed


LAYOUT_PREFIX = "[benchmark][lifecycle_merge_layout]"
TRIAL_PREFIX = "[benchmark][lifecycle_merge_trial]"


def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def env_bool(name: str, default: bool) -> bool:
    value = env(name)
    if not value:
        return default
    return value.lower() in {"1", "true", "yes", "y"}


def emit(prefix: str, payload: dict[str, Any]) -> None:
    print(f"{prefix} " + json.dumps(payload, sort_keys=True, default=str))


def table_detail(spark: SparkSession, path: str) -> dict[str, Any]:
    row = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").collect()[0].asDict(recursive=True)
    return {
        "path": path,
        "table_bytes": int(row.get("sizeInBytes") or 0),
        "table_files": int(row.get("numFiles") or 0),
        "partition_columns": list(row.get("partitionColumns") or []),
    }


def data_file_stats(spark: SparkSession, path: str, months: set[str] | None = None) -> dict[str, int]:
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
    iterator = fs.listFiles(hadoop_path, True)
    files = 0
    size_bytes = 0

    while iterator.hasNext():
        status = iterator.next()
        file_path = status.getPath().toString()
        if "/_delta_log/" in file_path or not file_path.endswith(".parquet"):
            continue
        if months and not any(f"year_month={month}" in file_path for month in months):
            continue
        files += 1
        size_bytes += int(status.getLen())

    return {"candidate_files": files, "candidate_bytes": size_bytes}


def query_specs() -> list[tuple[str, list[str] | None]]:
    month = env("BENCHMARK_FILTER_MONTH", "2024-01")
    quarter = [
        value.strip()
        for value in env("BENCHMARK_FILTER_QUARTER", "2024-01,2024-02,2024-03").split(",")
        if value.strip()
    ]
    specs = [
        ("single_month", [month]),
        ("multi_month", quarter),
    ]
    if env_bool("BENCHMARK_MERGE_INCLUDE_ALL_MONTHS", True):
        specs.insert(0, ("all_months", None))
    return specs


def month_selectivity(months: list[str] | None, all_months: list[str]) -> float:
    if not all_months:
        return 0.0
    if months is None:
        return 1.0
    return round(len(set(months).intersection(all_months)) / len(set(all_months)), 6)


def filter_completed(completed: DataFrame, months: list[str] | None) -> DataFrame:
    if not months:
        return completed
    return completed.filter(F.col("year_month").isin(months))


def write_target(target: DataFrame, path: str, partitioned: bool) -> None:
    writer = target.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partitioned:
        writer = writer.partitionBy("year_month")
    writer.save(path)


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def merge_condition(months: list[str] | None) -> str:
    conditions = [
        "target.year_month = source.year_month",
        "target.trip_id = source.trip_id",
    ]
    if months:
        month_literals = ", ".join(sql_string(month) for month in months)
        conditions.insert(1, f"target.year_month IN ({month_literals})")
    return " AND ".join(conditions)


def merge_completed(target_path: str, updates: DataFrame, months: list[str] | None) -> dict[str, Any]:
    target = DeltaTable.forPath(updates.sparkSession, target_path)
    (
        target.alias("target")
        .merge(
            updates.alias("source"),
            merge_condition(months),
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
    history = target.history(1).select("operationMetrics").collect()[0]["operationMetrics"] or {}
    return {key: int(value) if str(value).isdigit() else value for key, value in history.items()}


def trial_target_path(root: str, query: str, trial: int) -> str:
    return f"{root}/{query}/trial_{trial}"


def run_layout(
    spark: SparkSession,
    layout: str,
    target_df: DataFrame,
    updates_df: DataFrame,
    source_rows: int,
    query: str,
    months: list[str] | None,
    trial: int,
    root: str,
    all_months: list[str],
) -> dict[str, Any]:
    path = trial_target_path(root, query, trial)
    write_target(target_df, path, partitioned=layout == "partitioned")
    detail = table_detail(spark, path)
    month_set = set(months) if months else None
    candidate = (
        data_file_stats(spark, path, month_set)
        if layout == "partitioned"
        else data_file_stats(spark, path)
    )

    started = time.perf_counter()
    metrics = merge_completed(path, updates_df, months)
    duration_ms = int((time.perf_counter() - started) * 1000)
    result_rows = int(spark.read.format("delta").load(path).count())
    return {
        "layout": layout,
        "query": query,
        "trial": trial,
        "duration_ms": duration_ms,
        "filter_months": months or [],
        "selectivity": month_selectivity(months, all_months),
        "total_months": len(set(all_months)),
        "source_rows": source_rows,
        "result_rows": result_rows,
        **detail,
        **candidate,
        **{f"operation_{key}": value for key, value in metrics.items()},
    }


def run_merge_benchmark(spark: SparkSession) -> None:
    source_path = env("BENCHMARK_INPUT_PATH")
    partitioned_root = env("BENCHMARK_PARTITIONED_PATH")
    unpartitioned_root = env("BENCHMARK_UNPARTITIONED_PATH")
    trials = int(env("BENCHMARK_MERGE_TRIALS", "3"))
    if not source_path or not partitioned_root or not unpartitioned_root:
        raise ValueError(
            "BENCHMARK_INPUT_PATH, BENCHMARK_PARTITIONED_PATH, and "
            "BENCHMARK_UNPARTITIONED_PATH are required"
        )

    completed = spark.read.format("delta").load(source_path)
    target = lifecycle_from_completed(completed)
    all_months = [
        row["year_month"]
        for row in completed.select("year_month").where(F.col("year_month").isNotNull()).distinct().collect()
    ]
    target_rows = int(target.count())
    emit(
        LAYOUT_PREFIX,
        {
            "source_path": source_path,
            "target_seed_rows": target_rows,
            "total_months": len(set(all_months)),
            "months": sorted(set(all_months)),
            "partitioned_root": partitioned_root,
            "unpartitioned_root": unpartitioned_root,
        },
    )

    for query, months in query_specs():
        updates = lifecycle_from_completed(filter_completed(completed, months))
        source_rows = int(updates.count())
        for trial in range(1, trials + 1):
            layouts = [
                ("partitioned", partitioned_root),
                ("unpartitioned", unpartitioned_root),
            ]
            if trial % 2 == 0:
                layouts.reverse()
            for layout, root in layouts:
                emit(
                    TRIAL_PREFIX,
                    run_layout(
                        spark,
                        layout,
                        target,
                        updates,
                        source_rows,
                        query,
                        months,
                        trial,
                        root,
                        all_months,
                    ),
                )


def main() -> None:
    action = env("BENCHMARK_ACTION")
    if action != "lifecycle_merge":
        raise ValueError("BENCHMARK_ACTION must be lifecycle_merge")
    spark = build_spark_session("batch")
    try:
        run_merge_benchmark(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
