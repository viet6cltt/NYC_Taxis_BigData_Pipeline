-- =============================================================================
-- NYC Taxi BI — Sample Queries for Superset SQL Lab
-- Database: Trino - NYC Taxi Lakehouse | Catalog: delta
-- =============================================================================

-- 1. Registered schemas
SHOW SCHEMAS FROM delta;

-- 2. Silver lifecycle status
SELECT
    status,
    COUNT(*) AS trip_count
FROM delta.silver_nyc_taxi.trip_lifecycle
GROUP BY status
ORDER BY status;

-- 3. Completed trip overview by month
SELECT
    year_month,
    COUNT(*) AS completed_trips,
    ROUND(SUM(total_amount), 0) AS total_revenue,
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    ROUND(AVG(trip_distance), 2) AS avg_distance_miles,
    ROUND(AVG(CAST(trip_duration_seconds AS DOUBLE)) / 60.0, 1) AS avg_duration_min
FROM delta.silver_nyc_taxi.trip_lifecycle
WHERE status = 'completed'
GROUP BY year_month
ORDER BY year_month;

-- 4. Demand/revenue by pickup hour
SELECT
    trip_hour AS pickup_hour,
    COUNT(*) AS completed_trips,
    ROUND(SUM(total_amount), 0) AS total_revenue,
    ROUND(AVG(fare_amount), 2) AS avg_fare
FROM delta.silver_nyc_taxi.trip_lifecycle
WHERE status = 'completed'
GROUP BY trip_hour
ORDER BY trip_hour;

-- 5. Top pickup zones
SELECT
    pulocation_id,
    COUNT(*) AS completed_trips,
    ROUND(SUM(total_amount), 0) AS total_revenue,
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    ROUND(AVG(trip_distance), 2) AS avg_distance_miles
FROM delta.silver_nyc_taxi.trip_lifecycle
WHERE status = 'completed'
GROUP BY pulocation_id
ORDER BY completed_trips DESC
LIMIT 10;

-- 6. Top pickup/dropoff routes
SELECT
    pulocation_id,
    dolocation_id,
    COUNT(*) AS completed_trips,
    ROUND(SUM(total_amount), 0) AS total_revenue,
    ROUND(AVG(fare_amount), 2) AS avg_fare
FROM delta.silver_nyc_taxi.trip_lifecycle
WHERE status = 'completed'
GROUP BY pulocation_id, dolocation_id
ORDER BY completed_trips DESC
LIMIT 10;

-- 7. Payment type distribution
SELECT
    payment_type_desc,
    COUNT(*) AS completed_trips,
    ROUND(SUM(total_amount), 0) AS total_revenue,
    ROUND(AVG(tip_amount), 2) AS avg_tip
FROM delta.silver_nyc_taxi.trip_lifecycle
WHERE status = 'completed'
GROUP BY payment_type_desc
ORDER BY completed_trips DESC;

-- 8. Gold ML feature table sanity check
SELECT
    year_month,
    COUNT(*) AS training_rows,
    ROUND(AVG(fare_amount), 2) AS avg_label_fare,
    ROUND(AVG(estimated_trip_distance), 2) AS avg_estimated_distance,
    ROUND(AVG(CAST(estimated_trip_duration_seconds AS DOUBLE)) / 60.0, 1) AS avg_estimated_duration_min
FROM delta.gold_ml.features
GROUP BY year_month
ORDER BY year_month;

-- 9. Realtime predictions by model version
SELECT
    model_name,
    model_version,
    COUNT(*) AS prediction_count,
    ROUND(AVG(predicted_fare_amount), 2) AS avg_predicted_fare
FROM delta.gold_ml.predictions
GROUP BY model_name, model_version
ORDER BY prediction_count DESC;

-- 10. Prediction vs actual errors
SELECT
    COUNT(*) AS evaluated_predictions,
    ROUND(AVG(absolute_error), 2) AS mae,
    ROUND(SQRT(AVG(squared_error)), 2) AS rmse,
    ROUND(AVG(predicted_fare_amount - actual_fare_amount), 2) AS bias,
    ROUND(AVG(label_delay_seconds), 1) AS avg_label_delay_seconds
FROM delta.gold_ml.prediction_actuals;

-- 11. Daily model quality for dashboard
SELECT
    metric_date,
    prediction_count,
    mae,
    rmse,
    bias
FROM delta.gold_monitoring.model_quality_daily
ORDER BY metric_date DESC;

-- 12. Realtime prediction coverage by estimate fallback level
SELECT
    estimate_level,
    COUNT(*) AS prediction_count,
    ROUND(AVG(predicted_fare_amount), 2) AS avg_predicted_fare
FROM delta.gold_ml.predictions
GROUP BY estimate_level
ORDER BY prediction_count DESC;

-- 13. Model quality by estimate fallback level
SELECT
    estimate_level,
    COUNT(*) AS evaluated_predictions,
    ROUND(AVG(absolute_error), 2) AS mae,
    ROUND(SQRT(AVG(squared_error)), 2) AS rmse,
    ROUND(AVG(prediction_error), 2) AS bias
FROM delta.gold_ml.prediction_actuals
GROUP BY estimate_level
ORDER BY mae DESC;

-- 14. Route-level error hotspots
SELECT
    pulocation_id,
    dolocation_id,
    COUNT(*) AS evaluated_predictions,
    ROUND(AVG(absolute_error), 2) AS mae,
    ROUND(AVG(prediction_error), 2) AS bias
FROM delta.gold_ml.prediction_actuals
GROUP BY pulocation_id, dolocation_id
HAVING COUNT(*) >= 10
ORDER BY mae DESC
LIMIT 10;
