#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from statistics import median
from typing import Any


PREFIXES = {
    "preflight": "[benchmark][processing_preflight] ",
    "inspect": "[benchmark][processing_inspect] ",
    "layout": "[benchmark][pruning_layout] ",
    "plan": "[benchmark][pruning_plan] ",
    "pruning_trial": "[benchmark][pruning_trial] ",
    "merge_layout": "[benchmark][lifecycle_merge_layout] ",
    "merge_trial": "[benchmark][lifecycle_merge_trial] ",
}


def load_prefixed_json(log_dir: Path) -> dict[str, list[dict[str, Any]]]:
    payloads = {key: [] for key in PREFIXES}
    for log_path in sorted(log_dir.glob("*.log")):
        for line in log_path.read_text(errors="replace").splitlines():
            for key, prefix in PREFIXES.items():
                if prefix not in line:
                    continue
                try:
                    payload = json.loads(line.split(prefix, 1)[1])
                except json.JSONDecodeError:
                    continue
                payload["source_log"] = str(log_path)
                payloads[key].append(payload)
    return payloads


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    if not rows:
        path.write_text("")
        return
    keys = fieldnames or list(rows[0].keys())
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def load_stage_runs(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def mib(value: int | float | None) -> float:
    return round(float(value or 0) / 1024.0 / 1024.0, 2)


def rate(value: int | float | None, duration_ms: int | float | None) -> float:
    seconds = float(duration_ms or 0) / 1000.0
    return round(float(value or 0) / seconds, 2) if seconds > 0 else 0.0


def inspection_map(payloads: dict[str, list[dict[str, Any]]]) -> dict[str, dict[str, Any]]:
    inspections = {payload["path"]: payload for payload in payloads["inspect"]}
    for payload in payloads["preflight"]:
        inspections[payload["path"]] = {
            **payload,
            "rows": payload.get("rows_in_expected_year", 0),
            "year_months": sorted(payload.get("month_counts", {})),
        }
    return inspections


def processing_rows(stage_runs: list[dict[str, Any]], inspections: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for run in stage_runs:
        input_stats = inspections.get(run["input_path"], {})
        output_stats = inspections.get(run["output_path"], {})
        output_rows = int(output_stats.get("rows") or 0)
        output_size = int(output_stats.get("size_bytes") or 0)
        duration_ms = int(run["duration_ms"])
        rows.append(
            {
                **run,
                "input_rows": int(input_stats.get("rows") or 0),
                "output_rows": output_rows,
                "output_size_bytes": output_size,
                "output_size_mib": mib(output_size),
                "output_data_files": int(output_stats.get("data_files") or 0),
                "output_partition_columns": ",".join(output_stats.get("partition_columns") or []),
                "rows_per_sec": rate(output_rows, duration_ms),
                "mib_per_sec": rate(output_size / 1024.0 / 1024.0, duration_ms),
            }
        )
    return rows


def summarize_processing(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[row["stage"]].append(row)

    summaries = []
    for stage, stage_rows in grouped.items():
        durations = [int(row["duration_ms"]) for row in stage_rows]
        row_rates = [float(row["rows_per_sec"]) for row in stage_rows]
        mib_rates = [float(row["mib_per_sec"]) for row in stage_rows]
        summaries.append(
            {
                "stage": stage,
                "trials": len(stage_rows),
                "duration_min_ms": min(durations),
                "duration_median_ms": median(durations),
                "duration_max_ms": max(durations),
                "output_rows_median": median([int(row["output_rows"]) for row in stage_rows]),
                "output_size_mib_median": median([float(row["output_size_mib"]) for row in stage_rows]),
                "rows_per_sec_median": median(row_rates),
                "mib_per_sec_median": median(mib_rates),
            }
        )
    return sorted(summaries, key=lambda item: item["stage"])


def value_close(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is right
    return abs(float(left) - float(right)) <= 1e-9 * max(1.0, abs(float(left)), abs(float(right)))


def assert_pruning_results_match(rows: list[dict[str, Any]]) -> None:
    grouped: dict[tuple[str, int], dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in rows:
        grouped[(row["query"], int(row["trial"]))][row["layout"]] = row

    for (query, trial), layouts in grouped.items():
        if set(layouts) != {"partitioned", "unpartitioned"}:
            raise RuntimeError(f"Missing pruning layout for query={query}, trial={trial}")
        partitioned = layouts["partitioned"]
        unpartitioned = layouts["unpartitioned"]
        for field in ["rows", "avg_fare_amount", "avg_estimated_trip_distance"]:
            if not value_close(partitioned.get(field), unpartitioned.get(field)):
                raise RuntimeError(
                    f"Pruning query results differ for {query} trial {trial} field {field}: "
                    f"{partitioned.get(field)} != {unpartitioned.get(field)}"
                )


def normalize_pruning_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = []
    for row in rows:
        normalized.append(
            {
                "query": row["query"],
                "layout": row["layout"],
                "trial": int(row["trial"]),
                "duration_ms": int(row["duration_ms"]),
                "filter_months": ",".join(row.get("filter_months") or []),
                "selectivity": row.get("selectivity"),
                "total_months": row.get("total_months"),
                "rows": row.get("rows"),
                "avg_fare_amount": row.get("avg_fare_amount"),
                "avg_estimated_trip_distance": row.get("avg_estimated_trip_distance"),
                "table_files": row.get("table_files"),
                "table_bytes": row.get("table_bytes"),
                "candidate_files": row.get("candidate_files"),
                "candidate_bytes": row.get("candidate_bytes"),
            }
        )
    return normalized


def summarize_pruning(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(row["query"], row["layout"])].append(row)

    medians: dict[tuple[str, str], float] = {}
    for key, group_rows in grouped.items():
        medians[key] = float(median([int(row["duration_ms"]) for row in group_rows]))

    summaries = []
    for (query, layout), group_rows in grouped.items():
        first = group_rows[0]
        baseline_ms = medians.get((query, "unpartitioned"), 0.0)
        layout_ms = medians[(query, layout)]
        summaries.append(
            {
                "query": query,
                "layout": layout,
                "trials": len(group_rows),
                "median_ms": layout_ms,
                "min_ms": min(int(row["duration_ms"]) for row in group_rows),
                "max_ms": max(int(row["duration_ms"]) for row in group_rows),
                "candidate_files": first.get("candidate_files"),
                "candidate_bytes": first.get("candidate_bytes"),
                "candidate_mib": mib(first.get("candidate_bytes")),
                "table_files": first.get("table_files"),
                "table_bytes": first.get("table_bytes"),
                "table_mib": mib(first.get("table_bytes")),
                "selectivity": first.get("selectivity"),
                "speedup_vs_unpartitioned": round(baseline_ms / layout_ms, 3) if layout_ms else 0.0,
            }
        )
    return sorted(summaries, key=lambda item: (item["query"], item["layout"]))


def normalize_merge_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = []
    for row in rows:
        normalized.append(
            {
                **row,
                "trial": int(row["trial"]),
                "duration_ms": int(row["duration_ms"]),
                "filter_months": ",".join(row.get("filter_months") or []),
                "partition_columns": ",".join(row.get("partition_columns") or []),
            }
        )
    return normalized


def assert_merge_results_match(rows: list[dict[str, Any]]) -> None:
    grouped: dict[tuple[str, int], dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in rows:
        grouped[(row["query"], int(row["trial"]))][row["layout"]] = row

    for (query, trial), layouts in grouped.items():
        if set(layouts) != {"partitioned", "unpartitioned"}:
            raise RuntimeError(f"Missing lifecycle MERGE layout for query={query}, trial={trial}")
        if int(layouts["partitioned"]["result_rows"]) != int(layouts["unpartitioned"]["result_rows"]):
            raise RuntimeError(f"Lifecycle MERGE result rows differ for query={query}, trial={trial}")


def summarize_merge(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(row["query"], row["layout"])].append(row)

    medians = {
        key: float(median([int(row["duration_ms"]) for row in group_rows]))
        for key, group_rows in grouped.items()
    }
    summaries = []
    for (query, layout), group_rows in grouped.items():
        first = group_rows[0]
        layout_ms = medians[(query, layout)]
        baseline_ms = medians.get((query, "unpartitioned"), 0.0)
        summaries.append(
            {
                "query": query,
                "layout": layout,
                "trials": len(group_rows),
                "median_ms": layout_ms,
                "min_ms": min(int(row["duration_ms"]) for row in group_rows),
                "max_ms": max(int(row["duration_ms"]) for row in group_rows),
                "source_rows": first.get("source_rows"),
                "candidate_files": first.get("candidate_files"),
                "candidate_bytes": first.get("candidate_bytes"),
                "candidate_mib": mib(first.get("candidate_bytes")),
                "table_files": first.get("table_files"),
                "table_bytes": first.get("table_bytes"),
                "table_mib": mib(first.get("table_bytes")),
                "selectivity": first.get("selectivity"),
                "speedup_vs_unpartitioned": round(baseline_ms / layout_ms, 3) if layout_ms else 0.0,
            }
        )
    return sorted(summaries, key=lambda item: (item["query"], item["layout"]))


def markdown_table(headers: list[str], rows: list[list[Any]]) -> str:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    lines.extend("| " + " | ".join(str(value) for value in row) + " |" for row in rows)
    return "\n".join(lines)


def write_plans(plan_dir: Path, plans: list[dict[str, Any]]) -> None:
    plan_dir.mkdir(parents=True, exist_ok=True)
    for payload in plans:
        path = plan_dir / f"{payload['query']}_{payload['layout']}.txt"
        path.write_text(payload["plan"])


def write_report(
    path: Path,
    args: argparse.Namespace,
    payloads: dict[str, list[dict[str, Any]]],
    processing_summary: list[dict[str, Any]],
    merge_summary: list[dict[str, Any]],
    pruning_summary: list[dict[str, Any]],
) -> None:
    bottleneck = max(processing_summary, key=lambda item: float(item["duration_median_ms"]), default=None)
    preflight = payloads["preflight"][0] if payloads["preflight"] else {}
    processing_table = markdown_table(
        ["Stage", "Trials", "Median ms", "Min ms", "Max ms", "Rows/s", "Output MiB"],
        [
            [
                row["stage"],
                row["trials"],
                round(float(row["duration_median_ms"]), 2),
                row["duration_min_ms"],
                row["duration_max_ms"],
                round(float(row["rows_per_sec_median"]), 2),
                round(float(row["output_size_mib_median"]), 2),
            ]
            for row in processing_summary
        ],
    )
    pruning_table = markdown_table(
        ["Query", "Layout", "Median ms", "Candidate files", "Candidate MiB", "Speedup"],
        [
            [
                row["query"],
                row["layout"],
                round(float(row["median_ms"]), 2),
                row["candidate_files"],
                row["candidate_mib"],
                f"{row['speedup_vs_unpartitioned']}x",
            ]
            for row in pruning_summary
        ],
    )
    merge_table = markdown_table(
        ["Scope", "Layout", "Median ms", "Source rows", "Candidate files", "Candidate MiB", "Speedup"],
        [
            [
                row["query"],
                row["layout"],
                round(float(row["median_ms"]), 2),
                row["source_rows"],
                row["candidate_files"],
                row["candidate_mib"],
                f"{row['speedup_vs_unpartitioned']}x",
            ]
            for row in merge_summary
        ],
    )
    missing = ", ".join(preflight.get("missing_months") or []) or "none"
    months = ", ".join(sorted((preflight.get("month_counts") or {}).keys()))
    bottleneck_text = bottleneck["stage"] if bottleneck else "n/a"
    path.write_text(
        f"""# NYC Taxi Processing Benchmark {args.run_id}

## Run

- Benchmark path: `{args.run_dir}`
- Bronze input: `{args.bronze_path}`
- Expected year: `{args.year}`
- Bronze year months seen: `{months}`
- Missing months: `{missing}`

## Processing

{processing_table}

The slowest median stage in this run is `{bottleneck_text}`. Stage times are wall-clock
runner times for isolated benchmark outputs, while throughput uses the inspected output
Delta table for each stage.

## Lifecycle MERGE layout benchmark

{merge_table}

The lifecycle benchmark seeds equivalent lifecycle targets from Silver completed,
then MERGEs completed updates into a target partitioned by `year_month` and an
unpartitioned target. `all_months` is the control scope; month-filtered scopes
show candidate target files/bytes before the MERGE.

## Delta partition pruning

{pruning_table}

`full_scan` is the control query. Filtered query speedups and reduced candidate
partition files/bytes are the pruning evidence. Spark physical plans are saved under
`plans/`; compare partitioned plans with unpartitioned plans for `year_month` filters.
"""
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", required=True, type=Path)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--year", required=True)
    parser.add_argument("--bronze-path", required=True)
    args = parser.parse_args()

    payloads = load_prefixed_json(args.run_dir / "logs")
    stage_runs = load_stage_runs(args.run_dir / "stage_runs.jsonl")
    inspections = inspection_map(payloads)
    processing_trials = processing_rows(stage_runs, inspections)
    processing_summary = summarize_processing(processing_trials)

    pruning_raw = payloads["pruning_trial"]
    assert_pruning_results_match(pruning_raw)
    pruning_trials = normalize_pruning_rows(pruning_raw)
    pruning_summary = summarize_pruning(pruning_trials)
    merge_raw = payloads["merge_trial"]
    assert_merge_results_match(merge_raw)
    merge_trials = normalize_merge_rows(merge_raw)
    merge_summary = summarize_merge(merge_trials)

    write_csv(args.run_dir / "processing_trials.csv", processing_trials)
    write_csv(args.run_dir / "processing_summary.csv", processing_summary)
    write_csv(args.run_dir / "pruning_trials.csv", pruning_trials)
    write_csv(args.run_dir / "pruning_summary.csv", pruning_summary)
    write_csv(args.run_dir / "merge_trials.csv", merge_trials)
    write_csv(args.run_dir / "merge_summary.csv", merge_summary)
    write_plans(args.run_dir / "plans", payloads["plan"])
    write_report(args.run_dir / "report.md", args, payloads, processing_summary, merge_summary, pruning_summary)

    print(json.dumps(
        {
            "processing_trials": len(processing_trials),
            "processing_stages": len(processing_summary),
            "pruning_trials": len(pruning_trials),
            "pruning_queries": len(pruning_summary),
            "merge_trials": len(merge_trials),
            "merge_scopes": len(merge_summary),
            "report": str(args.run_dir / "report.md"),
        },
        indent=2,
        sort_keys=True,
    ))


if __name__ == "__main__":
    main()
