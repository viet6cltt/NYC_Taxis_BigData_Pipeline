FROM apache/spark-py:v3.5.3

USER root

RUN pip install --no-cache-dir \
    delta-spark==3.2.0 \
    mlflow>=2.13.0 \
    xgboost>=2.0.0 \
    scikit-learn>=1.4.0 \
    pandas \
    numpy

WORKDIR /opt/spark/work-dir

COPY app/ ./app/
# Avro schema shared from ingestion layer
COPY schemas/ ./schemas/
