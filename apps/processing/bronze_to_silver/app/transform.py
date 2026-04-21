from pyspark.sql import DataFrame 
from pyspark.sql.functions import col, hour, date_format, unix_timestamp, when
from app.config import WATERMARK_DELAY
# Helper 
def add_payment_type_desc(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "payment_type_desc",
        when(col("payment_type") == 1, "credit_card")
        .when(col("payment_type") == 2, "cash")
        .when(col("payment_type") == 3, "no_charge")
        .when(col("payment_type") == 4, "dispute")
        .when(col("payment_type") == 5, "unknown")
        .when(col("payment_type") == 6, "voided_trip")
        .otherwise("other")
    )
    
def opt_transform_for_streaming(df: DataFrame) -> DataFrame:
    df = (
        df
        .withWatermark("event_time", WATERMARK_DELAY)
        .dropDuplicates(["event_id"])
    )
    return df

def transform(bronze_df: DataFrame, pipeline_mode: str) -> DataFrame:
    df = (
        bronze_df
        .withColumn("trip_hour", hour(col("pickup_datetime")))
        .withColumn("year_month", date_format(col("pickup_datetime"), "yyyy-MM"))
    )
    
    # Derive trip duration
    df = df.withColumn(
        "trip_duration_seconds",
        unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))
    )
    
    # Map payment type to description
    df = add_payment_type_desc(df)
    
    # Watermark + dedup 
    if pipeline_mode == "streaming":
        df = opt_transform_for_streaming(df)

    # Data quality check
    df = df.filter(col("event_id").isNotNull())
    df = df.filter(col("pickup_datetime").isNotNull())
    df = df.filter(col("dropoff_datetime").isNotNull())
    
    # time logic
    df = df.filter(col("dropoff_datetime") > col("pickup_datetime"))
    
    # numeric logic
    df = df.filter(col("trip_distance").isNotNull())
    df = df.filter(col("trip_distance") > 0)
    df = df.filter(col("trip_distance") < 1000)
    
    df = df.filter(col("fare_amount").isNotNull())
    df = df.filter(col("fare_amount") >= 0)

    df = df.filter(col("total_amount").isNotNull())
    df = df.filter(col("total_amount") >= 0)
    
    # passenger count logic
    df = df.filter(
        col("passenger_count").isNotNull() &
        ((col("passenger_count") > 0) & (col("passenger_count") < 10))
    )
    
    silver_df = df.select(
        col("event_id").alias("trip_id"),
        col("event_type"),
        col("schema_version"),
        col("ingest_mode"),
        col("ingest_timestamp"),
        col("event_time"),

        col("trip_date"),
        col("trip_hour"),
        col("year_month"),

        col("vendor_id"),
        col("pickup_datetime"),
        col("dropoff_datetime"),
        
        col("passenger_count"),
        col("trip_distance"),
        col("rate_code_id"),
        col("store_and_fwd_flag"),

        col("pulocation_id"),
        col("dolocation_id"),

        col("payment_type"),
        col("payment_type_desc"),

        col("fare_amount"),
        col("extra"),
        col("mta_tax"),
        col("tip_amount"),
        col("tolls_amount"),
        col("improvement_surcharge"),
        col("total_amount"),
        col("congestion_surcharge"),
        col("airport_fee")
    )
    
    return silver_df
    
    
