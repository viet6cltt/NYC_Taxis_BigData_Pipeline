FROM apache/spark:4.0.2-scala2.13-java17-python3-r-ubuntu

USER root

COPY ./training/train_xgboost/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /opt/spark/work-dir

COPY ./spark_jars /opt/spark/jars/

COPY ./training/train_xgboost/app/ ./app/
