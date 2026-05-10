import hashlib
import json
from datetime import datetime, timezone
from typing import Optional

from common.normalizer import normalize_row
from common.constants import (
    DROPOFF_DATETIME_FIELD,
    EVENT_TYPE_TRIP_COMPLETED,
    EVENT_TYPE_TRIP_STARTED,
    PICKUP_DATETIME_FIELD,
    SCHEMA_VERSION,
)


PAYLOAD_FIELDS = [
    "VendorID",
    PICKUP_DATETIME_FIELD,
    DROPOFF_DATETIME_FIELD,
    "passenger_count",
    "trip_distance",
    "trip_duration_seconds",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "Airport_fee",
]

STARTED_PAYLOAD_FIELDS = {
    "VendorID",
    PICKUP_DATETIME_FIELD,
    "passenger_count",
    "PULocationID",
    "DOLocationID",
}

TRIP_ID_FIELDS = [
    "VendorID",
    PICKUP_DATETIME_FIELD,
    DROPOFF_DATETIME_FIELD,
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "total_amount",
]


def _hash_payload(payload: dict) -> str:
    stable_json = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(stable_json.encode("utf-8")).hexdigest()


def _compute_trip_id(payload: dict, source_file: str) -> str:
    fingerprint = {"source_file": source_file}
    for field in TRIP_ID_FIELDS:
        fingerprint[field] = payload.get(field)
    return _hash_payload(fingerprint)


def _compute_event_id(event_type: str, trip_id: str) -> str:
    return _hash_payload({
        "schema_version": SCHEMA_VERSION,
        "event_type": event_type,
        "trip_id": trip_id,
    })


def _parse_timestamp(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def _compute_duration_seconds(payload: dict):
    pickup_time = _parse_timestamp(payload.get(PICKUP_DATETIME_FIELD))
    dropoff_time = _parse_timestamp(payload.get(DROPOFF_DATETIME_FIELD))
    if pickup_time is None or dropoff_time is None:
        return None
    duration_seconds = int((dropoff_time - pickup_time).total_seconds())
    return duration_seconds if duration_seconds > 0 else None


def _trip_date(event_time) -> Optional[str]:
    if event_time is None:
        return None
    event_time_str = str(event_time)
    return event_time_str[:10] if event_time_str else None


def _payload_for_event(payload: dict, event_type: str) -> dict:
    event_payload = {}
    for field in PAYLOAD_FIELDS:
        if event_type == EVENT_TYPE_TRIP_STARTED and field not in STARTED_PAYLOAD_FIELDS:
            event_payload[field] = None
        else:
            event_payload[field] = payload.get(field)

    if event_type == EVENT_TYPE_TRIP_COMPLETED:
        event_payload["trip_duration_seconds"] = _compute_duration_seconds(payload)

    return event_payload


def _build_event(row_dict: dict, ingest_mode: str, source_file: str, event_type: str) -> dict:
    payload = normalize_row(row_dict)
    trip_id = _compute_trip_id(payload, source_file)
    event_time = (
        payload.get(PICKUP_DATETIME_FIELD)
        if event_type == EVENT_TYPE_TRIP_STARTED
        else payload.get(DROPOFF_DATETIME_FIELD)
    )

    return {
        "metadata": {
            "event_id": _compute_event_id(event_type, trip_id),
            "event_type": event_type,
            "schema_version": SCHEMA_VERSION,
            "trip_id": trip_id,
            "source_file": source_file,
            "ingest_mode": ingest_mode,
            "ingest_timestamp": datetime.now(timezone.utc).isoformat(),
            "event_time": event_time,
            "trip_date": _trip_date(event_time),
        },
        "payload": _payload_for_event(payload, event_type),
    }


def build_trip_started_event(row_dict: dict, ingest_mode: str, source_file: str) -> dict:
    return _build_event(row_dict, ingest_mode, source_file, EVENT_TYPE_TRIP_STARTED)


def build_trip_completed_event(row_dict: dict, ingest_mode: str, source_file: str) -> dict:
    return _build_event(row_dict, ingest_mode, source_file, EVENT_TYPE_TRIP_COMPLETED)


def build_trip_event(row_dict: dict, ingest_mode: str, source_file: str) -> dict:
    return build_trip_completed_event(row_dict, ingest_mode, source_file)
