FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH=/app
EXPOSE 3000

CMD ["sh", "-c", "python import_model.py && bentoml serve service:svc --host 0.0.0.0 --port 3000"]
