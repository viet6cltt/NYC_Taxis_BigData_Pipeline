FROM apache/spark:4.0.2-scala2.13-java17-python3-r-ubuntu

USER root 

RUN apt-get update && apt-get install -y --no-install-recommends \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/spark/work-dir

# Copy Spark JAR files to the appropriate directory
COPY ./spark_jars /opt/spark/jars/

COPY ./processing/bronze_to_silver/app/ ./app/

RUN chmod 644 /opt/spark/jars/*.jar