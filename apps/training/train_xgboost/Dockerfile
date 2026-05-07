FROM apache/spark-py:v3.5.3

USER root

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /opt/spark/work-dir

COPY app/ ./app/
