FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

COPY ./ingestion/common/ /app/common/

COPY ./ingestion/streaming/replay_producer/app/ /app/app/
COPY ./ingestion/streaming/replay_producer/requirements.txt .
COPY ./ingestion/streaming/schemas/ /app/schemas/

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/app

CMD ["python", "app/main.py"]
