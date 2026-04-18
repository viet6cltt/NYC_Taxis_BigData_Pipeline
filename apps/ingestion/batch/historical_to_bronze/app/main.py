from pyspark.sql import SparkSession

from app.writer import write_to_bronze
from app.config import INPUT_PATH, OUTPUT_PATH, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
from app.transform import transform

def main():
    spark = SparkSession.builder.appName("HistoricalToBronze") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

    hadoop_conf = spark._jsc.hadoopConfiguration()

    hadoop_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
    hadoop_conf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    hadoop_conf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)

    # Cấu hình bổ trợ cho MinIO
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set("fs.s3a.attempts.maximum", "3")


    # Read the historical data in raw format
    raw_df = spark.read.parquet(INPUT_PATH)
    
    # Transform the raw data to bronze format
    bronze_df = transform(raw_df)
    
    # Write the transformed data to the bronze layer
    write_to_bronze(bronze_df, OUTPUT_PATH)
    
    spark.stop()
    
if __name__ == "__main__":
    main()