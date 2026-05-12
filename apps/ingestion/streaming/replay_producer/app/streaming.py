from app.kafka_producer import KafkaProducer
from app.avro_serializer import AvroSerializer
from app.event_builder import build_trip_completed_event, build_trip_started_event
from config import (
    KAFKA_COMPLETED_TOPIC,
    KAFKA_STARTED_TOPIC,
    BOOTSTRAP_SERVERS,
    STREAMING_SPEED_MULTIPLIER,
    STREAMING_BATCH_SIZE,
    STREAMING_MAX_SLEEP_SECONDS,
    LOOP_STREAMING,
    DATA_DIR,
    YEAR
)
from common.constants import INGEST_MODE_STREAMING

from datetime import datetime
import heapq
import time
import os 
import glob
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


EVENT_KIND_STARTED = "started"
EVENT_KIND_COMPLETED = "completed"


def iter_parquet_files(data_dir: str, year: str):
    # Mỗi tháng là một file parquet; sort để replay theo thứ tự ổn định.
    pattern = os.path.join(data_dir, year, "*.parquet")
    return sorted(glob.glob(pattern))


def normalize_event_ts(value):
    # Chuẩn hóa timestamp từ parquet về datetime để tính khoảng nghỉ giữa 2 event.
    if value is None:
        return None

    if isinstance(value, datetime):
        return value

    try:
        return datetime.fromisoformat(str(value))
    except Exception:
        return None


def is_valid_event_ts(ts: datetime, expected_year: int) -> bool:
    # Chỉ dùng timestamp hợp lệ và đúng năm đang replay.
    if ts is None:
        return False

    return ts.year == expected_year


def compute_sleep_seconds(prev_ts, curr_ts, speed_multiplier: float, max_sleep: float) -> float:
    # Mô phỏng tốc độ streaming bằng khoảng cách thời gian giữa 2 trip liên tiếp.
    if prev_ts is None or curr_ts is None:
        return 0.0

    if speed_multiplier <= 0:
        raise ValueError("speed_multiplier must be > 0")

    delta = (curr_ts - prev_ts).total_seconds()
    if delta <= 0:
        return 0.0

    sleep_seconds = delta / speed_multiplier
    return min(sleep_seconds, max_sleep)


def iter_streaming_events(data_dir: str, year: str, read_batch_size: int):
    files = iter_parquet_files(data_dir, year)
    if not files:
        raise FileNotFoundError(f"No parquet files found in {data_dir}/{year}")

    start_ts = datetime.fromisoformat(f"{year}-01-01 00:00:00")
    end_ts = datetime.fromisoformat(f"{year}-12-31 23:59:59")

    for file_path in files:
        file_name = os.path.basename(file_path)
        print(f"[streaming] Reading full file into RAM: {file_name}")

        # Đọc trọn 1 file, lọc dữ liệu ngoài năm cần replay rồi sort theo pickup time.
        table = pq.read_table(file_path)
        timestamp_col = table["tpep_pickup_datetime"]

        mask = pc.and_(
            pc.greater_equal(timestamp_col, pa.scalar(start_ts)),
            pc.less_equal(timestamp_col, pa.scalar(end_ts)),
        )
        filtered_table = table.filter(mask)

        sort_indices = pc.sort_indices(
            filtered_table,
            sort_keys=[("tpep_pickup_datetime", "ascending")],
        )
        sorted_table = filtered_table.take(sort_indices)

        print(
            f"[streaming] Prepared file={file_name}, "
            f"rows_before={table.num_rows}, rows_after={sorted_table.num_rows}"
        )

        completed_heap = []
        completed_seq = 0

        def emit_completed_until(cutoff_ts):
            while completed_heap and completed_heap[0][0] <= cutoff_ts:
                completed_ts, _, completed_row = heapq.heappop(completed_heap)
                yield completed_row, file_name, EVENT_KIND_COMPLETED, completed_ts

        for record_batch in sorted_table.to_batches(max_chunksize=read_batch_size):
            for row in record_batch.to_pylist():
                pickup_ts = normalize_event_ts(row.get("tpep_pickup_datetime"))
                dropoff_ts = normalize_event_ts(row.get("tpep_dropoff_datetime"))

                if pickup_ts is not None:
                    yield from emit_completed_until(pickup_ts)
                    yield row, file_name, EVENT_KIND_STARTED, pickup_ts
                else:
                    yield row, file_name, EVENT_KIND_STARTED, pickup_ts

                if dropoff_ts is not None:
                    heapq.heappush(completed_heap, (dropoff_ts, completed_seq, row))
                    completed_seq += 1

        while completed_heap:
            completed_ts, _, completed_row = heapq.heappop(completed_heap)
            yield completed_row, file_name, EVENT_KIND_COMPLETED, completed_ts

def run_streaming():
    # File validation
    files =iter_parquet_files(DATA_DIR, YEAR)
    if not files:
        raise FileNotFoundError(f"No parquet files found in {DATA_DIR}/{YEAR}")
    
    # Init Serializer 
    serializer = AvroSerializer("schemas/taxi_trip_event.avsc")
    
    # Init Kafka Producer 
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS
    )
    
    expected_year = int(YEAR)
    print(
        f"[streaming] Start replay year={YEAR}, files={len(files)}, "
        f"started_topic={KAFKA_STARTED_TOPIC}, completed_topic={KAFKA_COMPLETED_TOPIC}, "
        f"speed_multiplier={STREAMING_SPEED_MULTIPLIER}, "
        f"flush_every={STREAMING_BATCH_SIZE}"
    )
    
    try:
        while True:
            sent = 0
            skipped_bad_ts = 0
            buffered = 0
            sent_started = 0
            sent_completed = 0
            # Lưu event time hợp lệ gần nhất để tính sleep cho event tiếp theo.
            prev_event_ts = None
            
            for row, source_file, event_kind, current_event_ts in iter_streaming_events(
                DATA_DIR,
                YEAR,
                STREAMING_BATCH_SIZE,
            ):
                if not is_valid_event_ts(current_event_ts, expected_year):
                    print(
                        f"[streaming][WARN] Invalid {event_kind} event time={current_event_ts} "
                        f"(file={source_file})"
                    )
                    # Bỏ qua event có timestamp không hợp lệ hoặc không đúng năm, nhưng vẫn tiếp tục replay các event khác.
                    skipped_bad_ts += 1
                    continue

                sleep_seconds = compute_sleep_seconds(
                    prev_event_ts,
                    current_event_ts,
                    STREAMING_SPEED_MULTIPLIER,
                    STREAMING_MAX_SLEEP_SECONDS,
                )

                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)

                # Serializer lo phần metadata và chuyển giá trị về dạng Avro bytes.
                event_record = (
                    build_trip_started_event(
                        row,
                        ingest_mode=INGEST_MODE_STREAMING,
                        source_file=source_file,
                    )
                    if event_kind == EVENT_KIND_STARTED
                    else build_trip_completed_event(
                        row,
                        ingest_mode=INGEST_MODE_STREAMING,
                        source_file=source_file,
                    )
                )
                event = serializer.serialize(event_record)
                topic = (
                    KAFKA_STARTED_TOPIC
                    if event_kind == EVENT_KIND_STARTED
                    else KAFKA_COMPLETED_TOPIC
                )
                trip_id = event_record["metadata"]["trip_id"]

                producer.send(topic, key=trip_id.encode("utf-8"), value=event)
                sent += 1
                buffered += 1
                if event_kind == EVENT_KIND_STARTED:
                    sent_started += 1
                else:
                    sent_completed += 1

                # Flush theo lô nhỏ để giữ throughput ổn mà không dồn quá nhiều message trong bộ đệm.
                if buffered >= STREAMING_BATCH_SIZE:
                    producer.flush()
                    print(
                        f"[streaming] Sent={sent} "
                        f"(started={sent_started}, completed={sent_completed}), "
                        f"bad_ts={skipped_bad_ts}, "
                        f"last_valid_ts={prev_event_ts}"
                    )
                    print(
                        f"[streaming] Progress: {sent} events. "
                        f"kind={event_kind}, simulation_time={current_event_ts}"
                    )
                    buffered = 0

                prev_event_ts = current_event_ts

            if buffered > 0:
                # Flush phần còn lại ở cuối vòng replay.
                producer.flush()

            print(
                f"[streaming] Replay completed. Total sent={sent} "
                f"(started={sent_started}, completed={sent_completed}), "
                f"invalid_ts={skipped_bad_ts}"
            )

            if not LOOP_STREAMING:
                break
    finally: 
        producer.flush()
        producer.close()
