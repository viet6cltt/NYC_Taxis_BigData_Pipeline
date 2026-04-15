import uuid
from datetime import datetime, timezone 

import pandas as pd 

def to_json_safe(value):
    if pd.isna(value):
        return None
    
    if isinstance(value, pd.Timestamp):
        # parquet timestamp -> ISO string
        return value.isoformat()
    

    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass

    return value 

def build_event(row_dict: dict, ingest_mode: str, source_file: str) -> dict:  
    clean = {k: to_json_safe(v) for k, v in row_dict.items()}
    
    # Get time 
    pickup_time = clean.get("tpep_pickup_datetime")
    trip_date = pickup_time[:10] if pickup_time else None
    event = {
        "metadata": {
            "event_id": str(uuid.uuid4()),
            "event_type": "nyc_taxi_trip",
            "schema_version": "1.0",
            "source_file": source_file,
            "ingest_mode": ingest_mode,
            "ingest_timestamp": datetime.now(timezone.utc).isoformat(),
            "event_time": clean.get("tpep_pickup_datetime"),
            "trip_date": trip_date 
        },
        "payload": clean
    }
    
    return event