import uuid
from datetime import datetime, timezone

from common.normalizer import normalize_row
from common.constants import PICKUP_DATETIME_FIELD, SCHEMA_VERSION, EVENT_TYPE

def build_trip_event(row_dict: dict, ingest_mode: str, source_file: str) -> dict: 
    payload = normalize_row(row_dict)
    pickup_time = payload.get(PICKUP_DATETIME_FIELD)
    if pickup_time is None:
        trip_date = None
    else:
        pickup_time_str = str(pickup_time)
        trip_date = pickup_time_str[:10] if pickup_time_str else None
        
    return {
        "metadata": {
            "event_id": str(uuid.uuid4()),
            "event_type": EVENT_TYPE,
            "schema_version": SCHEMA_VERSION,
            "source_file": source_file,
            "ingest_mode": ingest_mode,
            "ingest_timestamp": datetime.now(timezone.utc).isoformat(),
            "event_time": pickup_time,
            "trip_date": trip_date
        },
        "payload": payload
    }

