import json 
from kafka import KafkaProducer
from config import BOOTSTRAP_SERVERS

def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda v: v.encode('utf-8') if v is not None else None,
        # batching
        linger_ms=100,
        batch_size=64*1024,
        compression_type='snappy',
        
        # performance / stability
        buffer_memory=64*1024*1024,
        acks=1,
        retries=3,
        request_timeout_ms=30000,
        max_block_ms=60000
        
    )