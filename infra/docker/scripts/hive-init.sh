#!/bin/bash
set -euo pipefail

TRINO="trino --server http://trino:8080 --user hive-init --output-format TSV --no-progress"

run_sql() {
  echo "  SQL: $1"
  echo "$1" | eval "$TRINO"
}

echo '=== Waiting for Trino SQL endpoint ==='
until echo "SELECT 1" | eval "$TRINO" >/dev/null 2>&1; do
  echo '  Trino HTTP is up but SQL is not ready yet; retrying in 3s...'
  sleep 3
done

register_delta_table() {
  local schema="$1"
  local table="$2"
  local location="$3"
  echo "  Registering delta.${schema}.${table} -> ${location}"
  echo "CALL delta.system.register_table(schema_name => '${schema}', table_name => '${table}', table_location => '${location}')" \
    | eval "$TRINO" \
    || echo "  Table may already be registered or source path is not ready yet: delta.${schema}.${table}"
}

echo '=== Creating Delta schemas ==='
run_sql "CREATE SCHEMA IF NOT EXISTS delta.bronze_nyc_taxi WITH (location = 's3a://lakehouse/_trino/bronze_nyc_taxi')"
run_sql "CREATE SCHEMA IF NOT EXISTS delta.silver_nyc_taxi WITH (location = 's3a://lakehouse/_trino/silver_nyc_taxi')"
run_sql "CREATE SCHEMA IF NOT EXISTS delta.gold_ml WITH (location = 's3a://lakehouse/_trino/gold_ml')"
run_sql "CREATE SCHEMA IF NOT EXISTS delta.gold_monitoring WITH (location = 's3a://lakehouse/_trino/gold_monitoring')"

echo '=== Registering Bronze/Silver Delta tables ==='
register_delta_table "bronze_nyc_taxi" "trip_started" "s3a://lakehouse/bronze/nyc-taxi/trip_started"
register_delta_table "bronze_nyc_taxi" "trip_completed" "s3a://lakehouse/bronze/nyc-taxi/trip_completed"
register_delta_table "silver_nyc_taxi" "trip_started" "s3a://lakehouse/silver/nyc-taxi/trip_started"
register_delta_table "silver_nyc_taxi" "trip_completed" "s3a://lakehouse/silver/nyc-taxi/trip_completed"
register_delta_table "silver_nyc_taxi" "trip_lifecycle" "s3a://lakehouse/silver/nyc-taxi/trip_lifecycle"

echo '=== Registering Gold ML Delta tables ==='
register_delta_table "gold_ml" "route_estimates" "s3a://lakehouse/gold/ml/route_estimates"
register_delta_table "gold_ml" "features" "s3a://lakehouse/gold/ml/features"
register_delta_table "gold_ml" "predictions" "s3a://lakehouse/gold/ml/predictions"
register_delta_table "gold_ml" "prediction_actuals" "s3a://lakehouse/gold/ml/prediction_actuals"
register_delta_table "gold_monitoring" "model_quality_daily" "s3a://lakehouse/gold/monitoring/model_quality_daily"

echo '=== Verify ==='
run_sql "SHOW TABLES IN delta.silver_nyc_taxi"
run_sql "SHOW TABLES IN delta.gold_ml"
run_sql "SHOW TABLES IN delta.gold_monitoring"
echo 'Delta tables registered.'
