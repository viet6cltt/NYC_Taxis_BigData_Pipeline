#!/bin/bash
set -e

# Configuration
SUPERSET_URL="http://localhost:8088"
USERNAME="admin"
PASSWORD="admin"
DATABASE_NAME="Trino - NYC Taxi Gold"

echo "=== NYC Taxi Dashboard Initialization ==="

# 1. Login to get Token
echo "Authenticating..."
LOGIN_RESPONSE=$(curl -s -X POST "$SUPERSET_URL/api/v1/security/login" \
  -H "Content-Type: application/json" \
  -d "{\"username\": \"$USERNAME\", \"password\": \"$PASSWORD\", \"provider\": \"db\", \"refresh\": true}")

TOKEN=$(python3 -c "import sys, json; print(json.loads(sys.argv[1])['access_token'])" "$LOGIN_RESPONSE")

if [ -z "$TOKEN" ]; then
    echo "❌ Authentication failed!"
    exit 1
fi

# 2. Find Database ID
echo "Finding Database ID..."
DB_LIST=$(curl -s -X GET "$SUPERSET_URL/api/v1/database/" -H "Authorization: Bearer $TOKEN")
DB_ID=$(python3 -c "import sys, json; data=json.loads(sys.argv[1]); print([d['id'] for d in data['result'] if d['database_name'] == '$DATABASE_NAME'][0])" "$DB_LIST")

echo "Found Database ID: $DB_ID"

# 3. Create Dataset (hive.gold.features)
echo "Creating Dataset: hive.gold.features..."
DATASET_PAYLOAD=$(cat <<EOF
{
  "database": $DB_ID,
  "schema": "gold",
  "table_name": "features"
}
EOF
)

DATASET_RESPONSE=$(curl -s -X POST "$SUPERSET_URL/api/v1/dataset/" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "$DATASET_PAYLOAD")

DATASET_ID=$(python3 -c "import sys, json; print(json.loads(sys.argv[1])['id'])" "$DATASET_RESPONSE" 2>/dev/null || echo "")

if [ -z "$DATASET_ID" ]; then
    echo "⚠️ Dataset may already exist, searching for it..."
    DATASET_LIST=$(curl -s -X GET "$SUPERSET_URL/api/v1/dataset/" -H "Authorization: Bearer $TOKEN")
    DATASET_ID=$(python3 -c "import sys, json; data=json.loads(sys.argv[1]); print([d['id'] for d in data['result'] if d['table_name'] == 'features'][0])" "$DATASET_LIST")
fi

echo "Dataset ID: $DATASET_ID"

# 4. Create a Sample Chart (Pie Chart: Passengers)
echo "Creating Sample Chart: Passenger Distribution..."
CHART_PARAMS='{"groupby":["passenger_count"],"metrics":["count"],"viz_type":"pie","color_scheme":"supersetColors"}'
CHART_PAYLOAD=$(cat <<EOF
{
  "chart_title": "Passenger Distribution (Jan 2024)",
  "viz_type": "pie",
  "datasource_id": $DATASET_ID,
  "datasource_type": "table",
  "params": "$CHART_PARAMS"
}
EOF
)

CHART_RESPONSE=$(curl -s -X POST "$SUPERSET_URL/api/v1/chart/" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "$CHART_PAYLOAD")

CHART_ID=$(python3 -c "import sys, json; print(json.loads(sys.argv[1])['id'])" "$CHART_RESPONSE")
echo "Chart ID: $CHART_ID"

# 5. Create Dashboard
echo "Creating Dashboard: NYC Taxi Analytics..."
DASHBOARD_PAYLOAD=$(cat <<EOF
{
  "dashboard_title": "NYC Taxi Analytics Dashboard",
  "published": true
}
EOF
)

DASH_RESPONSE=$(curl -s -X POST "$SUPERSET_URL/api/v1/dashboard/" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "$DASHBOARD_PAYLOAD")

DASH_ID=$(python3 -c "import sys, json; print(json.loads(sys.argv[1])['id'])" "$DASH_RESPONSE")
echo "Dashboard ID: $DASH_ID"

# 6. Add Chart to Dashboard (Simplified layout)
echo "Adding Chart to Dashboard..."
# This requires a PUT to dashboard with position_data, but for simplicity, 
# we can use the chart's 'dashboards' relationship
curl -s -X PUT "$SUPERSET_URL/api/v1/chart/$CHART_ID" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"dashboards\": [$DASH_ID]}"

echo "=========================================="
echo "✅ Dashboard 'NYC Taxi Analytics Dashboard' created!"
echo "📍 Access it at: $SUPERSET_URL/dashboard/list/"
echo "=========================================="
