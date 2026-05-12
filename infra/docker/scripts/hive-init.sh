#!/bin/bash
set -e

TRINO="trino --server http://trino:8080 --user hive-init --output-format TSV --no-progress"

run_sql() {
  echo "  SQL: $1"
  echo "$1" | eval $TRINO
}

echo '=== Creating schemas ==='
run_sql "CREATE SCHEMA IF NOT EXISTS hive.gold   WITH (location = 's3a://gold/')"
run_sql "CREATE SCHEMA IF NOT EXISTS hive.silver WITH (location = 's3a://silver/')"
run_sql "CREATE SCHEMA IF NOT EXISTS hive.bronze WITH (location = 's3a://bronze/')"

echo '=== Creating hive.gold.features ==='
run_sql "DROP TABLE IF EXISTS hive.gold.features"
run_sql "CREATE TABLE IF NOT EXISTS hive.gold.features (
  trip_id VARCHAR, fare_amount DOUBLE,
  passenger_count BIGINT, trip_distance DOUBLE, trip_duration_seconds BIGINT,
  pulocation_id INTEGER, dolocation_id INTEGER,
  pickup_hour INTEGER, pickup_day_of_week INTEGER, is_weekend INTEGER,
  hour_sin DOUBLE, hour_cos DOUBLE, day_sin DOUBLE, day_cos DOUBLE,
  distance_manhattan DOUBLE, speed DOUBLE,
  location_cluster INTEGER, temporal_cluster INTEGER,
  year_month VARCHAR
) WITH (format = 'PARQUET', external_location = 's3a://gold/features/data/')"

echo '=== Creating hive.gold.predictions ==='
run_sql "DROP TABLE IF EXISTS hive.gold.predictions"
run_sql "CREATE TABLE IF NOT EXISTS hive.gold.predictions (
  trip_id VARCHAR, passenger_count BIGINT, trip_distance DOUBLE,
  trip_duration_seconds BIGINT, speed DOUBLE,
  pickup_hour INTEGER, pickup_day_of_week INTEGER, is_weekend INTEGER,
  hour_sin DOUBLE, hour_cos DOUBLE, day_sin DOUBLE, day_cos DOUBLE,
  distance_manhattan DOUBLE, location_cluster INTEGER, temporal_cluster INTEGER,
  predicted_fare DOUBLE, model_version VARCHAR,
  prediction_timestamp TIMESTAMP, event_time TIMESTAMP
) WITH (format = 'PARQUET', external_location = 's3a://gold/predictions/')"

echo '=== Creating hive.silver.trips ==='
run_sql "DROP TABLE IF EXISTS hive.silver.trips"
run_sql "CREATE TABLE IF NOT EXISTS hive.silver.trips (
  trip_id VARCHAR, event_type VARCHAR, schema_version VARCHAR,
  ingest_mode VARCHAR, ingest_timestamp TIMESTAMP, event_time TIMESTAMP,
  trip_date VARCHAR, trip_hour INTEGER, vendor_id INTEGER,
  pickup_datetime TIMESTAMP, dropoff_datetime TIMESTAMP,
  passenger_count BIGINT, trip_distance DOUBLE, rate_code_id BIGINT,
  store_and_fwd_flag VARCHAR, pulocation_id INTEGER, dolocation_id INTEGER,
  payment_type INTEGER, payment_type_desc VARCHAR,
  fare_amount DOUBLE, extra DOUBLE, mta_tax DOUBLE, tip_amount DOUBLE,
  tolls_amount DOUBLE, improvement_surcharge DOUBLE, total_amount DOUBLE,
  congestion_surcharge DOUBLE, airport_fee DOUBLE,
  trip_duration_seconds BIGINT, year_month VARCHAR
) WITH (format = 'PARQUET', external_location = 's3a://silver/trips/data/')"

echo '=== Verify ==='
run_sql "SHOW TABLES IN hive.gold"
run_sql "SHOW TABLES IN hive.silver"
echo '✅ Hive tables registered!'
