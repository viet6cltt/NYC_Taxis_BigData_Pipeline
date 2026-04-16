from decimal import Decimal
import uuid
from datetime import date, datetime, timezone 

import pandas as pd 

def to_json_safe(value):
    # PyArrow to_pylist() trả về None cho null
    if value is None:
        return None
    
    # Handle datetime (PyArrow return datetime)
    if isinstance(value, datetime):
        return value.isoformat()
    
    if isinstance(value, date):
        return value.isoformat()
    
    if isinstance(value, Decimal):
        return float(value)
    
    if hasattr(value, "item"):
        return value.item()

    return value 

def build_event(row_dict: dict, ingest_mode: str, source_file: str) -> dict:  
    clean = {k: to_json_safe(v) for k, v in row_dict.items()}
    
    # Get time 
    pickup_time = clean.get("tpep_pickup_datetime")
    
    if pickup_time is None:
        trip_date = None
    else:
        pickup_time_str = str(pickup_time)
        trip_date = pickup_time_str[:10] if pickup_time_str else None
        
    event = {
        "metadata": {
            "event_id": str(uuid.uuid4()),
            "event_type": "nyc_taxi_trip",
            "schema_version": "1.0",
            "source_file": source_file,
            "ingest_mode": ingest_mode,
            "ingest_timestamp": datetime.now(timezone.utc).isoformat(),
            "event_time": pickup_time,
            "trip_date": trip_date 
        },
        "payload": clean
    }
    
    return event