FROM apache/spark-py:v3.5.3

USER root

# Install Python deps needed for KMeans inside executors
RUN pip install --no-cache-dir \
    delta-spark==3.2.0 \
    scikit-learn==1.4.2 \
    numpy \
    pandas

WORKDIR /opt/spark/work-dir

COPY app/ ./app/
