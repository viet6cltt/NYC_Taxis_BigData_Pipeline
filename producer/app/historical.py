import glob 
import os 

import pandas as pd 

from config import DATA_DIR, YEAR, HISTORICAL_FLUSH_EVERY, TOPIC
from kafka_client import build_producer
from serializer import build_event

def iter_parquet_files(data_dir: str, year: str):
    pattern = os.path.join(data_dir, f"{YEAR}", "*.parquet")
    return sorted(glob.glob(pattern))

def run_historical():
    files = iter_parquet_files(DATA_DIR, YEAR)
    if not files: 
        raise FileNotFoundError(f"No parquet files found in {DATA_DIR}/{YEAR}")
    
    producer = build_producer()
    sent = 0
    
    print(f"[historical] Start sending year={YEAR}, files={len(files)}, topic={TOPIC}")
    
    for file_path in files:
        file_name = os.path.basename(file_path)
        print(f"[historical] Processing file: {file_name}")
        
        df = pd.read_parquet(file_path)
        
        for idx, row in df.iterrows():
            event = build_event(row.to_dict(), ingest_mode="historical", source_file=file_name)
            producer.send(TOPIC, value=event)
            sent += 1
            
            if sent % HISTORICAL_FLUSH_EVERY == 0:
                producer.flush()
                print(f"[historical] Sent {sent} events so far...")
                
    producer.flush()
    print(f"[historical] Finished sending {sent} events for year={YEAR}")
