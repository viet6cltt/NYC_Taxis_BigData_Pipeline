#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
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
    for line in log_path.read_text(errors="replace").splitlines():
        if PREFIX not in line:
            continue
        payload = line.split(PREFIX, 1)[1].strip()
        try:
            rows.append(json.loads(payload))
        except json.JSONDecodeError:
            continue
    return rows


def summarize(args: argparse.Namespace) -> dict:
    progress_rows = [
        row
        for row in load_progress(args.spark_log)
        if int(row.get("num_input_rows") or 0) > 0
    ]

    input_rates = [float(row.get("input_rows_per_second") or 0.0) for row in progress_rows]
    processed_rates = [float(row.get("processed_rows_per_second") or 0.0) for row in progress_rows]
    rows_per_batch = [float(row.get("num_input_rows") or 0.0) for row in progress_rows]
    batch_durations = [float(row.get("trigger_execution_ms") or 0.0) for row in progress_rows]

    return {
        "scenario": args.scenario,
        "speed_multiplier": args.speed_multiplier,
        "max_offsets_per_trigger": args.max_offsets_per_trigger,
        "events_per_sec": round(mean(input_rates), 2) if input_rates else 0.0,
        "rows_per_batch": round(mean(rows_per_batch), 2) if rows_per_batch else 0.0,
        "avg_batch_duration_ms": round(mean(batch_durations), 2) if batch_durations else 0.0,
        "p95_batch_duration_ms": round(percentile(batch_durations, 95), 2),
        "processed_rows_per_sec": round(mean(processed_rates), 2) if processed_rates else 0.0,
        "microbatches": len(progress_rows),
        "notes": args.notes,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--speed-multiplier", required=True)
    parser.add_argument("--max-offsets-per-trigger", required=True)
    parser.add_argument("--spark-log", required=True, type=Path)
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
