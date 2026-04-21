from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from app.config import APP_NAME
def build_spark_session(pipeline_mode: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(APP_NAME)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    
    if pipeline_mode == "streaming":
        builder = builder.config("spark.sql.streaming.stateStore.providerClass",
                                 "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
    
    spark = builder.getOrCreate()
    
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set("fs.s3a.attempts.maximum", "3")
    
    return spark
