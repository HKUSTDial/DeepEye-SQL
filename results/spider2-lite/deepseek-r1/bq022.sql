WITH filtered_trips AS (
  SELECT trip_seconds, fare, NTILE(6) OVER (ORDER BY trip_seconds) AS quantile_group
  FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
  WHERE trip_seconds BETWEEN 0 AND 3600
)
SELECT quantile_group, ROUND(MIN(trip_seconds)/60.0) AS min_trip_minutes, ROUND(MAX(trip_seconds)/60.0) AS max_trip_minutes, COUNT(*) AS total_trips, AVG(fare) AS avg_fare
FROM filtered_trips
GROUP BY quantile_group
ORDER BY quantile_group