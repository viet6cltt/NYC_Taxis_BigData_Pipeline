from kafka import KafkaProducer

class TaxiKafkaProducer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: v,   # đã là bytes Avro
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            
            # performance / stability
            buffer_memory=64*1024*1024,
            acks=1,
            retries=3,
            request_timeout_ms=30000,
            max_block_ms=60000
        )

    def send(self, key: str, value: bytes):
        self.producer.send(self.topic, key=key, value=value)

    def flush(self):
        self.producer.flush()