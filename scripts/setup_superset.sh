#!/bin/bash
# Quick setup script for Superset on K8s

set -euo pipefail

NAMESPACE=${1:-lakehouse}
WAIT_TIME=${2:-60}

echo "🚀 Deploying Superset with Trino connection..."
echo "   Namespace: $NAMESPACE"
echo "   Wait timeout: ${WAIT_TIME}s"
echo ""

# Create namespace if not exists
echo "[1/5] Creating namespace..."
kubectl create namespace $NAMESPACE 2>/dev/null || echo "   (namespace already exists)"

# Deploy Superset stack
echo "[2/5] Deploying Superset stack..."
kubectl apply -f infra/k8s/superset/superset.yaml

# Wait for PostgreSQL
echo "[3/5] Waiting for PostgreSQL to be ready..."
kubectl wait --for=condition=ready pod -l app=postgres-superset -n $NAMESPACE --timeout=${WAIT_TIME}s || true

# Wait for Superset
echo "[4/5] Waiting for Superset to be ready..."
kubectl wait --for=condition=ready pod -l app=superset -n $NAMESPACE --timeout=${WAIT_TIME}s || true

# Wait for init job to complete
echo "[5/5] Waiting for Trino connection initialization..."
kubectl wait --for=condition=complete job/superset-init-trino -n $NAMESPACE --timeout=${WAIT_TIME}s || echo "   (init job still running, check logs later)"

echo ""
echo "✅ Superset deployment complete!"
echo ""
echo "📊 Next steps:"
echo ""
echo "1. Port-forward Superset UI:"
echo "   kubectl port-forward -n $NAMESPACE svc/superset 8088:8088"
echo ""
echo "2. Access UI:"
echo "   http://localhost:8088"
echo ""
echo "3. Login:"
echo "   Username: admin"
echo "   Password: admin"
echo ""
echo "4. Verify Trino connection:"
echo "   Admin → Databases → Should see 'trino' database"
echo ""
echo "5. Run SQL query in SQL Lab:"
echo "   SELECT * FROM iceberg.silver.trips LIMIT 5"
echo ""
echo "📝 Check logs:"
echo "   kubectl logs -n $NAMESPACE -l app=superset -f"
echo "   kubectl logs -n $NAMESPACE job/superset-init-trino"
echo ""
echo "⚠️  Remember:"
echo "   - Change admin password in production!"
echo "   - Update SUPERSET_SECRET_KEY!"
echo ""
