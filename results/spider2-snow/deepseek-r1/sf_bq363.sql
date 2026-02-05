WITH trips_with_minutes AS (
  SELECT 
    "trip_seconds",
    "fare",
    FLOOR("trip_seconds"/60) AS "duration_minutes"
  FROM "CHICAGO"."CHICAGO_TAXI_TRIPS"."TAXI_TRIPS"
  WHERE "trip_seconds" >= 60 AND "trip_seconds" < 3060
    AND FLOOR("trip_seconds"/60) BETWEEN 1 AND 50
),
distinct_minutes AS (
  SELECT DISTINCT "duration_minutes"
  FROM trips_with_minutes
),
quantile_assignment AS (
  SELECT 
    "duration_minutes",
    NTILE(10) OVER (ORDER BY "duration_minutes") AS "quantile_group"
  FROM distinct_minutes
),
trips_with_quantiles AS (
  SELECT 
    t."duration_minutes",
    t."fare",
    q."quantile_group"
  FROM trips_with_minutes t
  JOIN quantile_assignment q ON t."duration_minutes" = q."duration_minutes"
),
quantile_stats AS (
  SELECT
    "quantile_group",
    MIN("duration_minutes") AS "min_minutes",
    MAX("duration_minutes") AS "max_minutes",
    COUNT(*) AS "total_trips",
    ROUND(AVG("fare"), 2) AS "avg_fare"
  FROM trips_with_quantiles
  GROUP BY "quantile_group"
)
SELECT
  LPAD("min_minutes", 2, '0') || 'm to ' || LPAD("max_minutes", 2, '0') || 'm' AS "time_range",
  "total_trips",
  "avg_fare"
FROM quantile_stats
ORDER BY "min_minutes"