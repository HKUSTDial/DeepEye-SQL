WITH trips_with_minutes AS (
  SELECT 
    trip_seconds,
    fare,
    unique_key,
    DIV(trip_seconds, 60) AS minutes
  FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
  WHERE trip_seconds BETWEEN 60 AND 3000
),
distinct_minutes AS (
  SELECT DISTINCT minutes
  FROM trips_with_minutes
  WHERE minutes BETWEEN 1 AND 50
),
minute_quantiles AS (
  SELECT 
    minutes,
    NTILE(10) OVER (ORDER BY minutes) AS quantile_group
  FROM distinct_minutes
),
trips_with_quantile AS (
  SELECT 
    t.trip_seconds,
    t.fare,
    t.unique_key,
    t.minutes,
    q.quantile_group
  FROM trips_with_minutes t
  INNER JOIN minute_quantiles q ON t.minutes = q.minutes
)
SELECT 
  CONCAT(LPAD(CAST(MIN(minutes) AS STRING), 2, '0'), 'm to ', LPAD(CAST(MAX(minutes) AS STRING), 2, '0'), 'm') AS time_range,
  COUNT(*) AS total_trips,
  ROUND(AVG(fare), 2) AS average_fare
FROM trips_with_quantile
GROUP BY quantile_group
ORDER BY MIN(minutes)