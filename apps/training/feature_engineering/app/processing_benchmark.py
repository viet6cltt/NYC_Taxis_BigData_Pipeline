"""
Spark helpers for batch processing and Delta pruning benchmarks.

This entrypoint runs inside the feature-engineering image so the benchmark
runner can inspect Delta paths without adding another Spark image.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Iterable
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


PRECHECK_PREFIX = "[benchmark][processing_preflight]"
INSPECT_PREFIX = "[benchmark][processing_inspect]"
LAYOUT_PREFIX = "[benchmark][pruning_layout]"
PLAN_PREFIX = "[benchmark][pruning_plan]"
TRIAL_PREFIX = "[benchmark][pruning_trial]"


def emit(prefix: str, payload: dict[str, Any]) -> None:
    print(f"{prefix} " + json.dumps(payload, sort_keys=True, default=str))


def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def env_bool(name: str, default: bool) -> bool:
    value = env(name)
    if not value:
        return default
    return value.lower() in {"1", "true", "yes", "y"}


def build_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("NYCTaxi-ProcessingBenchmark")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", env("MINIO_ENDPOINT", "http://minio-api.storage.svc.cluster.local:9000"))
    hadoop_conf.set("fs.s3a.access.key", env("MINIO_ACCESS_KEY", "minioadmin"))
    hadoop_conf.set("fs.s3a.secret.key", env("MINIO_SECRET_KEY", "minioadmin"))
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set("fs.s3a.attempts.maximum", "3")
    return spark


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    if not path:
        raise ValueError("BENCHMARK_INPUT_PATH is required")
    return spark.read.format("delta").load(path)


def table_detail(spark: SparkSession, path: str) -> dict[str, Any]:
    row = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").collect()[0].asDict(recursive=True)
    return {
        "path": path,
        "size_bytes": int(row.get("sizeInBytes") or 0),
        "data_files": int(row.get("numFiles") or 0),
        "partition_columns": list(row.get("partitionColumns") or []),
    }


def inspect_delta(spark: SparkSession) -> None:
    path = env("BENCHMARK_INPUT_PATH")
    df = read_delta(spark, path)
    detail = table_detail(spark, path)
    months = []
    if "year_month" in df.columns:
        months = [
            row["year_month"]
            for row in df.select("year_month").where(F.col("year_month").isNotNull()).distinct().orderBy("year_month").collect()
        ]
    emit(
        INSPECT_PREFIX,
        {
            **detail,
            "rows": int(df.count()),
            "year_months": months,
        },
    )


def expected_months(year: str) -> list[str]:
    return [f"{year}-{month:02d}" for month in range(1, 13)]


def preflight_bronze(spark: SparkSession) -> None:
    path = env("BENCHMARK_INPUT_PATH")
    year = env("BENCHMARK_EXPECTED_YEAR", "2024")
    require_full_year = env_bool("BENCHMARK_REQUIRE_FULL_YEAR", True)
    df = read_delta(spark, path)
    if "pickup_datetime" not in df.columns:
        raise ValueError(f"Bronze path is missing pickup_datetime: {path}")

    month_col = F.date_format(F.col("pickup_datetime").cast("timestamp"), "yyyy-MM")
    per_month = (
        df.withColumn("_year_month", month_col)
        .filter(F.col("_year_month").startswith(f"{year}-"))
        .groupBy("_year_month")
        .count()
        .orderBy("_year_month")
        .collect()
    )
    month_counts = {row["_year_month"]: int(row["count"]) for row in per_month}
    missing = [month for month in expected_months(year) if month not in month_counts]
    detail = table_detail(spark, path)
    payload = {
        **detail,
        "expected_year": year,
        "month_counts": month_counts,
        "missing_months": missing,
        "require_full_year": require_full_year,
        "rows_in_expected_year": sum(month_counts.values()),
    }
    emit(PRECHECK_PREFIX, payload)
    if require_full_year and missing:
        raise RuntimeError(
            f"Bronze benchmark path {path} is missing months for {year}: {', '.join(missing)}"
        )


def data_file_stats(spark: SparkSession, path: str, months: set[str] | None = None) -> dict[str, int]:
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
    iterator = fs.listFiles(hadoop_path, True)
    data_files = 0
    size_bytes = 0

    while iterator.hasNext():
        status = iterator.next()
        file_path = status.getPath().toString()
        if "/_delta_log/" in file_path or not file_path.endswith(".parquet"):
            continue
        if months and not any(f"year_month={month}" in file_path for month in months):
            continue
        data_files += 1
        size_bytes += int(status.getLen())

    return {"files": data_files, "bytes": size_bytes}


def filtered_aggregate(df: DataFrame, months: list[str] | None) -> DataFrame:
    query_df = df
    if months:
        query_df = query_df.filter(F.col("year_month").isin(months))
    return query_df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.avg("fare_amount").alias("avg_fare_amount"),
        F.avg("estimated_trip_distance").alias("avg_estimated_trip_distance"),
    )


def physical_plan(df: DataFrame) -> str:
    return str(df._jdf.queryExecution().executedPlan().toString())


def write_pruning_layouts(spark: SparkSession, source_path: str, partitioned_path: str, unpartitioned_path: str) -> DataFrame:
    features = read_delta(spark, source_path)
    features.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("year_month").save(
        partitioned_path
    )
    features.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(unpartitioned_path)

    source_rows = int(features.count())
    partitioned_rows = int(read_delta(spark, partitioned_path).count())
    unpartitioned_rows = int(read_delta(spark, unpartitioned_path).count())
    if source_rows != partitioned_rows or source_rows != unpartitioned_rows:
        raise RuntimeError(
            "Pruning layout copies changed row count: "
            f"source={source_rows}, partitioned={partitioned_rows}, unpartitioned={unpartitioned_rows}"
        )
    emit(
        LAYOUT_PREFIX,
        {
            "source_path": source_path,
            "source_rows": source_rows,
            "partitioned": {**table_detail(spark, partitioned_path), "rows": partitioned_rows},
            "unpartitioned": {**table_detail(spark, unpartitioned_path), "rows": unpartitioned_rows},
        },
    )
    return read_delta(spark, partitioned_path)


def query_specs() -> list[tuple[str, list[str] | None]]:
    month = env("BENCHMARK_FILTER_MONTH", "2024-01")
    quarter = [value.strip() for value in env("BENCHMARK_FILTER_QUARTER", "2024-01,2024-02,2024-03").split(",")]
    quarter = [value for value in quarter if value]
    return [
        ("full_scan", None),
        ("single_month", [month]),
        ("multi_month", quarter),
    ]


def emit_query_plan(layout: str, query_name: str, query_df: DataFrame) -> None:
    emit(
        PLAN_PREFIX,
        {
            "layout": layout,
            "query": query_name,
            "plan": physical_plan(query_df),
        },
    )


def selectivity(months: list[str] | None, all_months: list[str]) -> float:
    if not all_months:
        return 0.0
    if months is None:
        return 1.0
    selected = set(months).intersection(all_months)
    return round(len(selected) / len(set(all_months)), 6)


def iterate_layouts(layouts: Iterable[tuple[str, str]]) -> list[tuple[str, str]]:
    return list(layouts)


def run_pruning(spark: SparkSession) -> None:
    source_path = env("BENCHMARK_INPUT_PATH")
    partitioned_path = env("BENCHMARK_PARTITIONED_PATH")
    unpartitioned_path = env("BENCHMARK_UNPARTITIONED_PATH")
    trials = int(env("BENCHMARK_PRUNING_TRIALS", "3"))
    if not partitioned_path or not unpartitioned_path:
        raise ValueError("BENCHMARK_PARTITIONED_PATH and BENCHMARK_UNPARTITIONED_PATH are required")

    partitioned_df = write_pruning_layouts(spark, source_path, partitioned_path, unpartitioned_path)
    all_months = [
        row["year_month"]
        for row in partitioned_df.select("year_month").where(F.col("year_month").isNotNull()).distinct().collect()
    ]
    layouts = iterate_layouts([("partitioned", partitioned_path), ("unpartitioned", unpartitioned_path)])
    table_stats = {layout: data_file_stats(spark, path) for layout, path in layouts}

    for query_name, months in query_specs():
        month_set = set(months) if months else None
        candidate_stats = {
            "partitioned": data_file_stats(spark, partitioned_path, month_set),
            "unpartitioned": table_stats["unpartitioned"],
        }

        for layout, path in layouts:
            aggregate = filtered_aggregate(read_delta(spark, path), months)
            aggregate.collect()
            emit_query_plan(layout, query_name, aggregate)

        for trial in range(1, trials + 1):
            round_layouts = layouts if trial % 2 else list(reversed(layouts))
            for layout, path in round_layouts:
                aggregate = filtered_aggregate(read_delta(spark, path), months)
                started = time.perf_counter()
                result = aggregate.collect()[0].asDict(recursive=True)
                duration_ms = int((time.perf_counter() - started) * 1000)
                emit(
                    TRIAL_PREFIX,
                    {
                        "layout": layout,
                        "query": query_name,
                        "trial": trial,
                        "duration_ms": duration_ms,
                        "filter_months": months or [],
                        "selectivity": selectivity(months, all_months),
                        "total_months": len(set(all_months)),
                        "table_files": table_stats[layout]["files"],
                        "table_bytes": table_stats[layout]["bytes"],
                        "candidate_files": candidate_stats[layout]["files"],
                        "candidate_bytes": candidate_stats[layout]["bytes"],
                        **result,
                    },
                )


def main() -> None:
    spark = build_spark_session()
    action = env("BENCHMARK_ACTION")
    try:
        if action == "preflight":
            preflight_bronze(spark)
        elif action == "inspect":
            inspect_delta(spark)
        elif action == "pruning":
            run_pruning(spark)
        else:
            raise ValueError("BENCHMARK_ACTION must be one of: preflight, inspect, pruning")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
