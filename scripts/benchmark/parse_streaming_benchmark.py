#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import datetime
from pathlib import Path
from statistics import mean


PREFIX = "[benchmark][spark_progress] "


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round((pct / 100.0) * (len(ordered) - 1))))
    return ordered[index]


def load_progress(log_path: Path) -> list[dict]:
    rows = []
    if not log_path.exists():
        return rows
    for line in log_path.read_text(errors="replace").splitlines():
        if PREFIX not in line:
            continue
        payload = line.split(PREFIX, 1)[1].strip()
        try:
            rows.append(json.loads(payload))
        except json.JSONDecodeError:
            continue
    return rows


def parse_progress_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def drop_warmup(rows: list[dict], warmup_seconds: int) -> list[dict]:
    if warmup_seconds <= 0 or not rows:
        return rows
    first_ts = parse_progress_timestamp(rows[0].get("timestamp"))
    if first_ts is None:
        return rows
    cutoff = first_ts.timestamp() + warmup_seconds
    warmed = [
        row
        for row in rows
        if (parse_progress_timestamp(row.get("timestamp")) or first_ts).timestamp() >= cutoff
    ]
    return warmed or rows


def parse_kafka_lag(log_path: Path) -> dict[str, float | int]:
    if not log_path.exists():
        return {
            "avg_kafka_lag": 0.0,
            "max_kafka_lag": 0,
            "final_kafka_lag": 0,
            "kafka_lag_samples": 0,
        }

    sample_lags: list[int] = []
    current_total = 0
    current_has_rows = False

    for raw_line in log_path.read_text(errors="replace").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("# timestamp="):
            if current_has_rows:
                sample_lags.append(current_total)
            current_total = 0
            current_has_rows = False
            continue
        parts = line.split()
        if len(parts) < 6 or parts[0] in {"GROUP", "Consumer"}:
            continue
        try:
            lag = int(parts[5])
        except ValueError:
            continue
        current_total += lag
        current_has_rows = True

    if current_has_rows:
        sample_lags.append(current_total)

    return {
        "avg_kafka_lag": round(mean(sample_lags), 2) if sample_lags else 0.0,
        "max_kafka_lag": max(sample_lags) if sample_lags else 0,
        "final_kafka_lag": sample_lags[-1] if sample_lags else 0,
        "kafka_lag_samples": len(sample_lags),
    }


def sum_topic_offsets(offsets: object) -> int | None:
    if not isinstance(offsets, dict):
        return None

    total = 0
    found = False
    for partitions in offsets.values():
        if not isinstance(partitions, dict):
            continue
        for offset in partitions.values():
            try:
                total += int(offset)
            except (TypeError, ValueError):
                continue
            found = True
    return total if found else None


def parse_spark_offset_lag(progress_rows: list[dict]) -> dict[str, float | int]:
    sample_lags: list[int] = []

    for row in progress_rows:
        end_offset = sum_topic_offsets(row.get("end_offset"))
        latest_offset = sum_topic_offsets(row.get("latest_offset"))
        if end_offset is None or latest_offset is None:
            continue
        sample_lags.append(max(0, latest_offset - end_offset))

    return {
        "avg_kafka_lag": round(mean(sample_lags), 2) if sample_lags else 0.0,
        "max_kafka_lag": max(sample_lags) if sample_lags else 0,
        "final_kafka_lag": sample_lags[-1] if sample_lags else 0,
        "kafka_lag_samples": len(sample_lags),
    }


def parse_quantity(value: str) -> float:
    value = value.strip()
    match = re.fullmatch(r"([0-9.]+)([A-Za-z]+)?", value)
    if not match:
        return 0.0
    number = float(match.group(1))
    unit = match.group(2) or ""
    if unit == "n":
        return number / 1_000_000.0
    if unit == "u":
        return number / 1_000.0
    if unit == "m":
        return number
    if unit in {"Ki", "KiB"}:
        return number / 1024.0
    if unit in {"Mi", "MiB"}:
        return number
    if unit in {"Gi", "GiB"}:
        return number * 1024.0
    return number


def parse_resources(log_path: Path) -> dict[str, float | int]:
    if not log_path.exists():
        return {
            "avg_cpu_mcores": 0.0,
            "max_cpu_mcores": 0.0,
            "avg_memory_mib": 0.0,
            "max_memory_mib": 0.0,
            "resource_samples": 0,
        }

    sample_cpu: list[float] = []
    sample_memory: list[float] = []
    current_cpu = 0.0
    current_memory = 0.0
    current_has_rows = False

    for raw_line in log_path.read_text(errors="replace").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("# timestamp="):
            if current_has_rows:
                sample_cpu.append(current_cpu)
                sample_memory.append(current_memory)
            current_cpu = 0.0
            current_memory = 0.0
            current_has_rows = False
            continue
        parts = line.split()
        if len(parts) < 4 or parts[0] == "POD" or "error" in line.lower():
            continue
        current_cpu += parse_quantity(parts[-2])
        current_memory += parse_quantity(parts[-1])
        current_has_rows = True

    if current_has_rows:
        sample_cpu.append(current_cpu)
        sample_memory.append(current_memory)

    return {
        "avg_cpu_mcores": round(mean(sample_cpu), 2) if sample_cpu else 0.0,
        "max_cpu_mcores": round(max(sample_cpu), 2) if sample_cpu else 0.0,
        "avg_memory_mib": round(mean(sample_memory), 2) if sample_memory else 0.0,
        "max_memory_mib": round(max(sample_memory), 2) if sample_memory else 0.0,
        "resource_samples": len(sample_cpu),
    }


def summarize(args: argparse.Namespace) -> dict:
    progress_rows = [
        row
        for row in load_progress(args.spark_log)
        if int(row.get("num_input_rows") or 0) > 0
    ]
    raw_microbatches = len(progress_rows)
    progress_rows = drop_warmup(progress_rows, args.warmup_seconds)

    input_rates = [float(row.get("input_rows_per_second") or 0.0) for row in progress_rows]
    processed_rates = [float(row.get("processed_rows_per_second") or 0.0) for row in progress_rows]
    rows_per_batch = [float(row.get("num_input_rows") or 0.0) for row in progress_rows]
    batch_durations = [float(row.get("trigger_execution_ms") or 0.0) for row in progress_rows]

    summary = {
        "scenario": args.scenario,
        "speed_multiplier": args.speed_multiplier,
        "max_offsets_per_trigger": args.max_offsets_per_trigger,
        "producer_replicas": args.producer_replicas,
        "kafka_partitions": args.kafka_partitions,
        "kafka_replicas": args.kafka_replicas,
        "spark_executors": args.spark_executors,
        "spark_shuffle_partitions": args.spark_shuffle_partitions,
        "warmup_seconds": args.warmup_seconds,
        "events_per_sec": round(mean(input_rates), 2) if input_rates else 0.0,
        "rows_per_batch": round(mean(rows_per_batch), 2) if rows_per_batch else 0.0,
        "avg_batch_duration_ms": round(mean(batch_durations), 2) if batch_durations else 0.0,
        "p95_batch_duration_ms": round(percentile(batch_durations, 95), 2),
        "processed_rows_per_sec": round(mean(processed_rates), 2) if processed_rates else 0.0,
        "microbatches": len(progress_rows),
        "raw_microbatches": raw_microbatches,
        "notes": args.notes,
    }
    consumer_group_lag = parse_kafka_lag(args.kafka_lag_log)
    spark_offset_lag = parse_spark_offset_lag(progress_rows)
    if consumer_group_lag["kafka_lag_samples"]:
        summary.update(consumer_group_lag)
        summary["kafka_lag_source"] = "consumer_group"
    else:
        summary.update(spark_offset_lag)
        summary["kafka_lag_source"] = "spark_progress_offsets"
    summary.update(parse_resources(args.resource_log))
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--speed-multiplier", required=True)
    parser.add_argument("--max-offsets-per-trigger", required=True)
    parser.add_argument("--producer-replicas", default="1")
    parser.add_argument("--kafka-partitions", default="6")
    parser.add_argument("--kafka-replicas", default="3")
    parser.add_argument("--spark-executors", default="1")
    parser.add_argument("--spark-shuffle-partitions", default="4")
    parser.add_argument("--warmup-seconds", type=int, default=0)
    parser.add_argument("--spark-log", required=True, type=Path)
    parser.add_argument("--kafka-lag-log", default=Path("/dev/null"), type=Path)
    parser.add_argument("--resource-log", default=Path("/dev/null"), type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--notes", default="")
    args = parser.parse_args()

    summary = summarize(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    exists = args.output.exists()
    with args.output.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(summary.keys()))
        if not exists:
            writer.writeheader()
        writer.writerow(summary)

    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
