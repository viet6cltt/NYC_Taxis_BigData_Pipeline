-- =============================================================================
-- NYC Taxi BI — Sample Queries cho Superset SQL Lab
-- Database: Trino - NYC Taxi Gold  |  Catalog: delta
-- =============================================================================

-- ----------------------------------------------------------------------------
-- 1. Tổng quan Gold Features
-- ----------------------------------------------------------------------------
SELECT
    year_month,
    COUNT(*)                          AS total_trips,
    ROUND(AVG(fare_amount), 2)        AS avg_fare_usd,
    ROUND(MIN(fare_amount), 2)        AS min_fare_usd,
    ROUND(MAX(fare_amount), 2)        AS max_fare_usd,
    ROUND(AVG(trip_distance), 2)      AS avg_distance_miles,
    ROUND(AVG(trip_duration_seconds) / 60.0, 1) AS avg_duration_min,
    ROUND(AVG(speed), 1)              AS avg_speed_mph
FROM delta.gold.features
GROUP BY year_month
ORDER BY year_month;

-- ----------------------------------------------------------------------------
-- 2. Phân phối chuyến đi theo giờ trong ngày
-- ----------------------------------------------------------------------------
SELECT
    pickup_hour,
    COUNT(*)                     AS trip_count,
    ROUND(AVG(fare_amount), 2)   AS avg_fare,
    ROUND(AVG(trip_distance), 2) AS avg_distance
FROM delta.gold.features
GROUP BY pickup_hour
ORDER BY pickup_hour;

-- ----------------------------------------------------------------------------
-- 3. Phân phối theo ngày trong tuần (0=Mon … 6=Sun)
-- ----------------------------------------------------------------------------
SELECT
    CASE pickup_day_of_week
        WHEN 0 THEN 'Monday'
        WHEN 1 THEN 'Tuesday'
        WHEN 2 THEN 'Wednesday'
        WHEN 3 THEN 'Thursday'
        WHEN 4 THEN 'Friday'
        WHEN 5 THEN 'Saturday'
        WHEN 6 THEN 'Sunday'
    END                              AS day_name,
    pickup_day_of_week,
    COUNT(*)                         AS trip_count,
    ROUND(AVG(fare_amount), 2)       AS avg_fare,
    ROUND(SUM(fare_amount), 0)       AS total_revenue
FROM delta.gold.features
GROUP BY pickup_day_of_week
ORDER BY pickup_day_of_week;

-- ----------------------------------------------------------------------------
-- 4. Weekday vs Weekend
-- ----------------------------------------------------------------------------
SELECT
    CASE is_weekend WHEN 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    COUNT(*)                         AS trip_count,
    ROUND(AVG(fare_amount), 2)       AS avg_fare,
    ROUND(AVG(trip_distance), 2)     AS avg_distance,
    ROUND(AVG(speed), 1)             AS avg_speed_mph
FROM delta.gold.features
GROUP BY is_weekend
ORDER BY is_weekend;

-- ----------------------------------------------------------------------------
-- 5. Top 10 pickup locations (by trip count)
-- ----------------------------------------------------------------------------
SELECT
    pulocation_id,
    COUNT(*)                   AS trip_count,
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    ROUND(AVG(trip_distance), 2) AS avg_distance
FROM delta.gold.features
GROUP BY pulocation_id
ORDER BY trip_count DESC
LIMIT 10;

-- ----------------------------------------------------------------------------
-- 6. Top 10 routes (pickup → dropoff)
-- ----------------------------------------------------------------------------
SELECT
    pulocation_id,
    dolocation_id,
    COUNT(*)                   AS trip_count,
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    ROUND(AVG(trip_distance), 2) AS avg_distance_miles
FROM delta.gold.features
GROUP BY pulocation_id, dolocation_id
ORDER BY trip_count DESC
LIMIT 10;

-- ----------------------------------------------------------------------------
-- 7. Fare distribution buckets
-- ----------------------------------------------------------------------------
SELECT
    CASE
        WHEN fare_amount < 5   THEN '< $5'
        WHEN fare_amount < 10  THEN '$5–$10'
        WHEN fare_amount < 20  THEN '$10–$20'
        WHEN fare_amount < 30  THEN '$20–$30'
        WHEN fare_amount < 50  THEN '$30–$50'
        ELSE '> $50'
    END                        AS fare_bucket,
    COUNT(*)                   AS trip_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
FROM delta.gold.features
GROUP BY 1
ORDER BY MIN(fare_amount);

-- ----------------------------------------------------------------------------
-- 8. Location clusters — đặc điểm từng cluster
-- ----------------------------------------------------------------------------
SELECT
    location_cluster,
    COUNT(*)                         AS trip_count,
    ROUND(AVG(fare_amount), 2)       AS avg_fare,
    ROUND(AVG(trip_distance), 2)     AS avg_distance,
    ROUND(AVG(speed), 1)             AS avg_speed,
    ROUND(AVG(trip_duration_seconds) / 60.0, 1) AS avg_duration_min
FROM delta.gold.features
GROUP BY location_cluster
ORDER BY location_cluster;

-- ----------------------------------------------------------------------------
-- 9. Temporal clusters — đặc điểm từng cluster thời gian
-- ----------------------------------------------------------------------------
SELECT
    temporal_cluster,
    CASE temporal_cluster
        WHEN 0 THEN 'Night (0–5h)'
        WHEN 1 THEN 'Morning (6–11h)'
        WHEN 2 THEN 'Afternoon (12–17h)'
        WHEN 3 THEN 'Evening (18–23h)'
    END                              AS time_period,
    COUNT(*)                         AS trip_count,
    ROUND(AVG(fare_amount), 2)       AS avg_fare,
    ROUND(AVG(trip_distance), 2)     AS avg_distance
FROM delta.gold.features
GROUP BY temporal_cluster
ORDER BY temporal_cluster;

-- ----------------------------------------------------------------------------
-- 10. Predictions vs Actual (nếu có dữ liệu streaming)
-- ----------------------------------------------------------------------------
SELECT
    DATE_TRUNC('hour', prediction_timestamp) AS hour_bucket,
    COUNT(*)                                  AS prediction_count,
    ROUND(AVG(predicted_fare), 2)             AS avg_predicted_fare,
    model_version
FROM delta.gold.predictions
GROUP BY 1, model_version
ORDER BY 1 DESC
LIMIT 48;

-- ----------------------------------------------------------------------------
-- 11. Monthly revenue trend
-- ----------------------------------------------------------------------------
SELECT
    year_month,
    COUNT(*)                         AS total_trips,
    ROUND(SUM(fare_amount), 0)       AS total_revenue,
    ROUND(AVG(fare_amount), 2)       AS avg_fare,
    ROUND(SUM(trip_distance), 0)     AS total_miles
FROM delta.gold.features
GROUP BY year_month
ORDER BY year_month;

-- ----------------------------------------------------------------------------
-- 12. Passenger count distribution
-- ----------------------------------------------------------------------------
SELECT
    passenger_count,
    COUNT(*)                   AS trip_count,
    ROUND(AVG(fare_amount), 2) AS avg_fare
FROM delta.gold.features
WHERE passenger_count BETWEEN 1 AND 6
GROUP BY passenger_count
ORDER BY passenger_count;
