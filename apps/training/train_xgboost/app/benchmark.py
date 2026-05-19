import json
import os
import time
import traceback
from contextlib import contextmanager
from typing import Any, Iterator


BENCHMARK_METRICS_ENABLED = os.getenv("BENCHMARK_METRICS_ENABLED", "true").lower() == "true"


def _emit(prefix: str, payload: dict[str, Any]) -> None:
    if BENCHMARK_METRICS_ENABLED:
        print(f"{prefix} " + json.dumps(payload, sort_keys=True))


@contextmanager
def benchmark_job(
    *,
    prefix: str,
    job_name: str,
    extra: dict[str, Any] | None = None,
) -> Iterator[dict[str, Any]]:
    metrics: dict[str, Any] = {}
    started_at = time.time()
    payload = {
        "metric_type": "spark_training_job",
        "job_name": job_name,
        "status": "started",
        "started_at_epoch_ms": int(started_at * 1000),
    }
    if extra:
        payload.update(extra)
    _emit(prefix, payload)

    try:
        yield metrics
    except Exception as exc:
        ended_at = time.time()
        error_payload = {
            **payload,
            **metrics,
            "status": "error",
            "ended_at_epoch_ms": int(ended_at * 1000),
            "duration_ms": int((ended_at - started_at) * 1000),
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "error_traceback": traceback.format_exc(limit=8),
        }
        _emit(prefix, error_payload)
        raise
    else:
        ended_at = time.time()
        success_payload = {
            **payload,
            **metrics,
            "status": "success",
            "ended_at_epoch_ms": int(ended_at * 1000),
            "duration_ms": int((ended_at - started_at) * 1000),
        }
        _emit(prefix, success_payload)
