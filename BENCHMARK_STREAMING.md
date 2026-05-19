# Streaming Benchmark Guide

This benchmark measures Kafka + Spark Structured Streaming throughput for the
NYC Taxi realtime ingestion path.

## What It Measures

| Metric | Source |
|---|---|
| Events/sec | Spark `inputRowsPerSecond` |
| Rows/batch | Spark `numInputRows` |
| Avg batch duration | Spark `durationMs.triggerExecution` |
| Processed rows/sec | Spark `processedRowsPerSecond` |
| CPU/RAM pod | `kubectl top pod --containers` and optional Prometheus/Grafana |
| Kafka consumer lag | `kafka-consumer-groups.sh --describe` best-effort samples |

Spark driver logs emit one JSON line per microbatch:

```text
[benchmark][spark_progress] {"batch_id": ..., "num_input_rows": ...}
```

## Build Images

Rebuild the replay producer and Spark streaming consumer after code changes:

```bash
bash scripts/images/build.sh replay
bash scripts/images/build.sh bronze_consumer
```

## Optional Observability Stack

Apply Prometheus, Grafana, Loki, and Promtail:

```bash
kubectl apply -f infra/k8s/monitoring/prometheus-grafana-loki.yaml
kubectl get pods -n monitoring
```

Access:

```bash
kubectl port-forward -n monitoring svc/grafana 30030:3000
kubectl port-forward -n monitoring svc/prometheus 30090:9090
```

Grafana: `http://localhost:30030`, user/password `admin` / `admin`.

Dashboard name: `NYC Taxi Streaming Benchmark`.

Loki query for Spark progress:

```logql
{namespace="lakehouse"} |= "[benchmark][spark_progress]"
```

Prometheus queries for resource usage:

```promql
sum(rate(container_cpu_usage_seconds_total{namespace=~"lakehouse|ingestion", container!="POD", image!=""}[1m]) and on(namespace,pod,container) (time() - container_last_seen{namespace=~"lakehouse|ingestion", container!="POD", image!=""} < 30)) by (namespace, pod)
sum(container_memory_working_set_bytes{namespace=~"lakehouse|ingestion", container!="POD", image!=""} and on(namespace,pod,container) (time() - container_last_seen{namespace=~"lakehouse|ingestion", container!="POD", image!=""} < 30)) by (namespace, pod)
```

## Run Benchmark

Default scenarios:

| Scenario | Speed multiplier | Max offsets/trigger |
|---|---:|---:|
| low | 1 | 500 |
| medium | 5 | 2000 |
| high | 20 | 10000 |

Run:

```bash
bash scripts/benchmark/run_streaming_benchmark.sh
```

Short test run:

```bash
BENCHMARK_RUN_SECONDS=60 \
BENCHMARK_SCENARIOS="low:1:500" \
bash scripts/benchmark/run_streaming_benchmark.sh
```

Custom full run:

```bash
BENCHMARK_RUN_SECONDS=300 \
BENCHMARK_TRIGGER_INTERVAL="10 seconds" \
BENCHMARK_SCENARIOS="low:1:500,medium:5:2000,high:20:10000" \
bash scripts/benchmark/run_streaming_benchmark.sh
```

Output directory:

```text
benchmark_results/streaming_<UTC timestamp>/
```

Important files:

| File | Description |
|---|---|
| `summary.csv` | Final report table |
| `<scenario>/spark_driver.log` | Spark microbatch progress logs |
| `<scenario>/spark_submit.log` | Spark submit client logs |
| `<scenario>/replay_job.log` | Producer replay logs |
| `<scenario>/resource_samples.tsv` | CPU/RAM samples from `kubectl top` |
| `<scenario>/kafka_lag_samples.txt` | Kafka consumer lag samples |

## Report Table

The generated `summary.csv` follows this shape:

| Scenario | Speed multiplier | Max offsets/trigger | Events/sec | Rows/batch | Avg batch duration | Processed rows/sec | Notes |
|---|---:|---:|---:|---:|---:|---:|---|

Example command to view it:

```bash
column -s, -t benchmark_results/streaming_*/summary.csv
```

## How To Interpret

Use these points in the benchmark conclusion:

- Kafka buffers events and decouples replay producer from Spark consumers.
- Spark Structured Streaming processes data in stable microbatches with checkpointing.
- Increasing `STREAMING_SPEED_MULTIPLIER` and `MAX_OFFSETS_PER_TRIGGER` increases throughput and rows per batch.
- Latency rises when batch duration grows, but the pipeline is healthy while `processed_rows_per_second` is close to or greater than `input_rows_per_second` and Kafka lag does not grow continuously.

## Notes

- If `kubectl top pod` fails, install or enable Kubernetes `metrics-server`.
- Kafka lag sampling is best-effort because Kafka images may expose CLI paths differently. The raw command output is still saved for evidence.
- Benchmark output writes to `s3a://lakehouse/benchmark/bronze/...` and checkpoint paths under `s3a://lakehouse/_checkpoints/benchmark/...`, so it does not overwrite production Bronze data.
