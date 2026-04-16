import glob
import os

import pyarrow.parquet as pq

from config import DATA_DIR, YEAR, HISTORICAL_FLUSH_EVERY, TOPIC
from kafka_client import build_producer
from serializer import build_event


def iter_parquet_files(data_dir: str, year: str):
    pattern = os.path.join(data_dir, year, "*.parquet")
    return sorted(glob.glob(pattern))


def run_historical(batch_size: int = 5000):
    files = iter_parquet_files(DATA_DIR, YEAR)
    if not files:
        raise FileNotFoundError(f"No parquet files found in {DATA_DIR}/{YEAR}")

    producer = build_producer()
    sent = 0

    print(f"[historical] Start sending year={YEAR}, files={len(files)}, topic={TOPIC}")

    try:
        for file_path in files:
            file_name = os.path.basename(file_path)
            print(f"[historical] Processing file: {file_name}")

            parquet_file = pq.ParquetFile(file_path)

            for record_batch in parquet_file.iter_batches(batch_size=batch_size):
                rows = record_batch.to_pylist()

                for row in rows:
                    event = build_event(
                        row,
                        ingest_mode="historical",
                        source_file=file_name
                    )
                    producer.send(TOPIC, value=event)
                    sent += 1

                    if sent % HISTORICAL_FLUSH_EVERY == 0:
                        producer.flush()
                        print(f"[historical] Sent {sent} events so far...")

        producer.flush()
        print(f"[historical] Finished sending {sent} events for year={YEAR}")

    finally:
        producer.flush()
        producer.close()