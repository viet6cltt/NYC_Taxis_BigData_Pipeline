#!/bin/bash
# Quick test script to verify Trino + Hive + Superset connections

NAMESPACE=${1:-lakehouse}

echo "🔍 Testing Trino + Hive + Superset Connections..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Test 1: Check if pods are running
echo "[Test 1] Checking pod status..."
echo ""
kubectl get pods -n $NAMESPACE -l app=trino,app=hive-metastore,app=superset,app=postgres-superset 2>/dev/null || \
kubectl get pods -n $NAMESPACE -o wide

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Test 2: Test Hive Metastore connectivity
echo "[Test 2] Testing Hive Metastore (port 9083)..."
kubectl run -it --rm --restart=Never --image=busybox:1.35 -n $NAMESPACE test-hive -- \
  sh -c "nc -zv hive-metastore.${NAMESPACE}.svc.cluster.local 9083" 2>&1 || echo "❌ Cannot reach Hive Metastore"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Test 3: Test Trino connectivity
echo "[Test 3] Testing Trino (port 8080)..."
kubectl run -it --rm --restart=Never --image=curlimages/curl -n $NAMESPACE test-trino -- \
  curl -s http://trino.${NAMESPACE}.svc.cluster.local:8080/v1/info | head -20

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Test 4: Test Trino query execution
echo "[Test 4] Testing Trino query (SELECT 1)..."
kubectl run -it --rm --restart=Never --image=trinodb/trino:480 -n $NAMESPACE test-trino-query -- \
  trino --server http://trino.${NAMESPACE}.svc.cluster.local:8080 --execute "SELECT 1 AS test" 2>&1 || echo "⚠️  Query test failed"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Test 5: Test Trino catalog list
echo "[Test 5] Checking Trino catalogs..."
kubectl run -it --rm --restart=Never --image=trinodb/trino:480 -n $NAMESPACE test-trino-catalogs -- \
  trino --server http://trino.${NAMESPACE}.svc.cluster.local:8080 --execute "SHOW CATALOGS" 2>&1 || echo "⚠️  Catalog check failed"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Test 6: Test PostgreSQL connectivity
echo "[Test 6] Testing PostgreSQL (port 5432)..."
kubectl run -it --rm --restart=Never --image=busybox:1.35 -n $NAMESPACE test-pg -- \
  sh -c "nc -zv postgres-superset.${NAMESPACE}.svc.cluster.local 5432" 2>&1 || echo "❌ Cannot reach PostgreSQL"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Test 7: Check Superset logs for Trino connection
echo "[Test 7] Checking Superset init job logs..."
kubectl logs -n $NAMESPACE job/superset-init-trino 2>/dev/null | tail -20 || echo "❌ Init job not found or not run yet"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Test 8: Port-forward and test Superset API
echo "[Test 8] Testing Superset API..."
echo "Attempting to port-forward Superset (30 seconds)..."
timeout 30 kubectl port-forward -n $NAMESPACE svc/superset 18088:8088 > /dev/null 2>&1 &
PF_PID=$!
sleep 3

if curl -s http://localhost:18088/health | grep -q "ok"; then
    echo "✅ Superset API is responding"
    
    # Check databases
    echo ""
    echo "Checking Superset databases..."
    curl -s http://localhost:18088/api/v1/databases | python3 -m json.tool 2>/dev/null | grep -A 5 "database_name" || echo "⚠️  Could not fetch databases"
else
    echo "❌ Superset API not responding"
fi

kill $PF_PID 2>/dev/null || true

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "✅ Test Summary:"
echo ""
echo "If all tests pass:"
echo "  1. Port-forward Superset: kubectl port-forward -n $NAMESPACE svc/superset 8088:8088"
echo "  2. Open: http://localhost:8088"
echo "  3. Login: admin / admin"
echo "  4. Check Admin → Databases → Should see 'trino'"
echo ""
echo "Run Trino query via Superset:"
echo "  SQL Lab → SELECT * FROM iceberg.silver.trips LIMIT 5"
echo ""
