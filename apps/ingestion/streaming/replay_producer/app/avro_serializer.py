import io
import json
from fastavro import parse_schema, schemaless_writer

class AvroSerializer:
    def __init__(self, schema_path: str):
        with open(schema_path, "r", encoding="utf-8") as f:
            self.schema = parse_schema(json.load(f))

    def serialize(self, record: dict) -> bytes:
        buffer = io.BytesIO()   
        schemaless_writer(buffer, self.schema, record)
        return buffer.getvalue()