FROM apache/spark:4.0.2-scala2.13-java17-python3-r-ubuntu

USER root 

RUN apt-get update && apt-get install -y --no-install-recommends \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/spark/work-dir

# Copy Spark JAR files to the appropriate directory
COPY ./spark_jars /opt/spark/jars/
# Add Spark Kafka connector for Spark 4.0.2 + Scala 2.13
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.2/spark-sql-kafka-0-10_2.13-4.0.2.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.2/spark-token-provider-kafka-0-10_2.13-4.0.2.jar /opt/spark/jars/

# Add Spark Avro module for from_avro / to_avro
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.13/4.0.2/spark-avro_2.13-4.0.2.jar /opt/spark/jars/

ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.0/kafka-clients-3.9.0.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar /opt/spark/jars/

# Cấu trúc là:
# /opt/spark/work-dir/app/
# /opt/spark/work-dir/schemas/
# /opt/spark/work-dir/common/
COPY common/ ./common/
COPY streaming/kafka_to_bronze/app/ ./app/
COPY streaming/schemas/ ./schemas/

RUN chmod 644 /opt/spark/jars/*.jar