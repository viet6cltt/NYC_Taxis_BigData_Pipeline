FROM apache/spark:4.0.2-scala2.13-java17-python3-r-ubuntu

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Python deps for Spark pandas UDF / mapInPandas / ML
RUN pip install --no-cache-dir \
    pyarrow==14.0.2 \
    scikit-learn==1.4.2 \
    numpy==1.26.4 \
    pandas==2.2.2

WORKDIR /opt/spark/work-dir

# Spark extra jars
COPY ./spark_jars /opt/spark/jars/

# App source
COPY ./training/feature_engineering/app/ ./app/

RUN chmod 644 /opt/spark/jars/*.jar

# Optional but recommended
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

USER spark