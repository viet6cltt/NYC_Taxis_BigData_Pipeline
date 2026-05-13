# Superset Quick Reference

## 🚀 Quick Deploy
```bash
# Deploy Superset
kubectl apply -f infra/k8s/superset/superset.yaml

# Or use convenience script
bash scripts/bi/setup_bi.sh

# With advanced (Redis + Celery workers)
kubectl apply -f infra/k8s/superset/superset.yaml
kubectl apply -f infra/k8s/superset/superset-advanced.yaml
```

## 📡 Port-forwarding & Access
```bash
# Port-forward to localhost
kubectl port-forward -n lakehouse svc/superset 8088:8088 &

# Or use NodePort (http://<node-ip>:30088)
kubectl get nodes -o wide

# Access URL
http://localhost:8088
```

## 🔑 Default Credentials
```
Username: admin
Password: admin
```

## 📊 UI Navigation
- **SQL Lab**: Write custom SQL queries
- **Dashboards**: View/create BI dashboards
- **Charts**: Create visualizations
- **Datasets**: Manage data sources
- **Admin Panel**: Manage databases, users, permissions

## 🗄️ Connect Trino Database
### Option 1: Auto (via Init Job)
- Already configured! Check: Admin → Databases → `trino`

### Option 2: Manual Connection
```
Database Name: trino
SQLAlchemy URI: trino://trino.lakehouse.svc.cluster.local:8080/delta
Engine Parameters: {}
```

## 🎯 Sample Queries
```sql
-- Query Bronze layer (raw completed events)
SELECT * FROM delta.bronze_nyc_taxi.trip_completed LIMIT 10

-- Query Silver lifecycle current-state table for BI-safe completed counts
SELECT COUNT(*) AS completed_trips
FROM delta.silver_nyc_taxi.trip_lifecycle
WHERE status = 'completed'

-- Query Gold ML features
SELECT * FROM delta.gold_ml.features WHERE year_month = '2024-01'

-- Check available tables
SHOW TABLES FROM delta.silver_nyc_taxi
```

## 🔧 Admin Commands
```bash
# SSH into Superset container
kubectl exec -it -n lakehouse deployment/superset -- bash

# Change admin password
superset set-password admin NEW_PASSWORD

# Create new admin user
superset fab create-admin --username newuser --password pass123 \
  --firstname First --lastname Last --email email@example.com

# List users
superset fab list-users

# Grant role to user
superset fab grant-role --user=newuser --role=Admin
```

## 📝 Important Config Variables
```yaml
SQLALCHEMY_DATABASE_URI: "postgresql://postgres:postgres@postgres-superset:5432/superset"
SUPERSET_SECRET_KEY: "your-secret-key-change-in-production"
REDIS_HOST: "redis-superset"  # (if using advanced deployment)
TRINO_HOST: "trino.lakehouse.svc.cluster.local"
TRINO_PORT: "8080"
```

## 🐛 Troubleshooting
```bash
# Check pod status
kubectl get pods -n lakehouse -l app=superset

# View logs
kubectl logs -n lakehouse -l app=superset

# View init job logs
kubectl logs -n lakehouse job/superset-add-trino-db

# Check if Trino is reachable
kubectl exec -it -n lakehouse deployment/superset -- \
  curl http://trino.lakehouse.svc.cluster.local:8080/v1/info

# Check PostgreSQL connection
kubectl exec -it -n lakehouse deployment/postgres-superset -- \
  psql -U postgres -d superset -c "SELECT 1"

# Restart Superset
kubectl rollout restart deployment/superset -n lakehouse
```

## 🔐 Production Checklist
- [ ] Change `SUPERSET_SECRET_KEY` to a strong random value
- [ ] Change admin password
- [ ] Setup HTTPS/TLS (use Ingress with cert)
- [ ] Enable LDAP/OAuth for authentication
- [ ] Configure backup for PostgreSQL
- [ ] Setup persistent storage for PostgreSQL (instead of emptyDir)
- [ ] Enable resource quotas
- [ ] Configure monitoring & alerting
- [ ] Setup log aggregation
- [ ] Enable audit logging

## 📚 Useful Links
- Superset Docs: https://superset.apache.org/
- Trino Connector: https://superset.apache.org/docs/databases/trino
- Iceberg Docs: https://iceberg.apache.org/

## 🎨 Dashboard Best Practices
1. Use meaningful chart titles
2. Add descriptions for context
3. Use consistent color schemes
4. Organize charts by business logic
5. Add filters for drill-down analysis
6. Cache queries for better performance
7. Use appropriate chart types (line, bar, pie, etc.)

## ⚡ Performance Tips
1. Optimize SQL queries (add WHERE clauses, LIMIT)
2. Use materialized views for complex queries
3. Enable result caching in Superset
4. Create dashboards with pre-computed aggregations
5. Use Superset caching layer with Redis
6. Monitor Trino query performance

## 🗑️ Cleanup/Uninstall
```bash
# Delete Superset deployment
kubectl delete -f infra/k8s/superset/superset.yaml

# Delete namespace (removes all resources)
kubectl delete namespace lakehouse

# Clean up resources
kubectl delete pvc --all -n lakehouse
```
