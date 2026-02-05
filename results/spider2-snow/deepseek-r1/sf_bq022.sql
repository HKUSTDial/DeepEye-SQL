WITH filtered_trips AS (
  SELECT 
    "unique_key",
    "fare",
    "trip_seconds",
    "trip_seconds" / 60.0 AS "trip_minutes"
  FROM 
    "CHICAGO"."CHICAGO_TAXI_TRIPS"."TAXI_TRIPS"
  WHERE 
    "trip_seconds" IS NOT NULL
    AND "trip_seconds" >= 0
    AND "trip_minutes" BETWEEN 0 AND 60
),
quantiled_trips AS (
  SELECT 
    *,
    NTILE(6) OVER (ORDER BY "trip_minutes") AS "quantile"
  FROM 
    filtered_trips
)
SELECT 
  "quantile",
  ROUND(MIN("trip_minutes")) AS "min_duration_minutes",
  ROUND(MAX("trip_minutes")) AS "max_duration_minutes",
  COUNT(*) AS "total_trips",
  ROUND(AVG("fare"), 2) AS "average_fare"
FROM 
  quantiled_trips
GROUP BY 
  "quantile"
ORDER BY 
  "quantile"