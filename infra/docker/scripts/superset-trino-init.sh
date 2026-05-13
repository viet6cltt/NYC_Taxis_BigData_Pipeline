#!/bin/sh
set -e

BASE=http://superset:8088

echo 'Logging in to Superset...'
LOGIN_RESP=$(curl -s -X POST "$BASE/api/v1/security/login" \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"admin","provider":"db","refresh":true}')

TOKEN=$(echo "$LOGIN_RESP" | grep -o '"access_token":"[^"]*' | cut -d'"' -f4)

if [ -z "$TOKEN" ]; then
  echo "ERROR: Could not get token. Response: $LOGIN_RESP"
  exit 1
fi
echo "  Token obtained."

echo 'Creating Trino database connection...'
HTTP_CODE=$(curl -s -o /tmp/db_resp.json -w "%{http_code}" \
  -X POST "$BASE/api/v1/database/" \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "database_name": "Trino - NYC Taxi Lakehouse",
    "sqlalchemy_uri": "trino://trino@trino:8080/delta",
    "expose_in_sqllab": true,
    "allow_run_async": true,
    "allow_ctas": false,
    "allow_cvas": false
  }')

echo "  HTTP status: $HTTP_CODE"
cat /tmp/db_resp.json
echo ""

if [ "$HTTP_CODE" = "201" ] || [ "$HTTP_CODE" = "200" ]; then
  echo "✅ Superset → Trino connection created!"
else
  echo "⚠️  Unexpected status $HTTP_CODE (may already exist, continuing...)"
fi

echo ""
echo "============================================"
echo " BI Stack Ready!"
echo "============================================"
echo " Superset:  http://localhost:8088  (admin/admin)"
echo " Trino UI:  http://localhost:8080"
echo " MinIO:     http://localhost:9001  (minioadmin/minioadmin)"
echo ""
echo " SQL Lab -> Database: Trino - NYC Taxi Lakehouse"
echo " Schema: silver_nyc_taxi -> trip_started, trip_completed, trip_lifecycle"
echo " Schema: gold_ml -> route_estimates, features, predictions, prediction_actuals"
echo " Schema: gold_monitoring -> model_quality_daily"
echo ""
echo " Sample query:"
echo "   SELECT year_month, COUNT(*) AS trips, ROUND(AVG(fare_amount),2) AS avg_fare"
echo "   FROM delta.silver_nyc_taxi.trip_lifecycle"
echo "   WHERE status = 'completed'"
echo "   GROUP BY year_month ORDER BY year_month;"
echo "============================================"
