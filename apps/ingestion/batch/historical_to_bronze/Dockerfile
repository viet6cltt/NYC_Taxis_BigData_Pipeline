FROM apache/spark:4.0.2-scala2.13-java17-python3-r-ubuntu

USER root 

RUN apt-get update && apt-get install -y --no-install-recommends \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/spark/work-dir

# Copy Spark JAR files to the appropriate directory
COPY ./spark_jars /opt/spark/jars/

# 4. Tổ chức lại code: Đưa cả 'app' và 'common' vào cùng một gốc
# Cấu trúc sau khi copy:
# /opt/spark/work-dir/app/main.py
# /opt/spark/work-dir/common/bronze_mapper.py
COPY common/ ./common/
COPY batch/historical_to_bronze/app/ ./app/

RUN chmod 644 /opt/spark/jars/*.jar