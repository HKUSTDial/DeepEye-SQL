WITH trip_data AS (
  SELECT 
    FORMAT_DATE('%Y%m', DATE(start_date)) AS year_month,
    duration_sec / 60.0 AS duration_minutes,
    start_date
  FROM `bigquery-public-data.san_francisco.bikeshare_trips`
),
windowed_data AS (
  SELECT
    year_month,
    duration_minutes,
    FIRST_VALUE(duration_minutes) OVER (PARTITION BY year_month ORDER BY start_date) AS first_trip_duration_minutes,
    LAST_VALUE(duration_minutes) OVER (PARTITION BY year_month ORDER BY start_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_trip_duration_minutes
  FROM trip_data
)
SELECT
  year_month,
  MIN(first_trip_duration_minutes) AS first_trip_duration_minutes,
  MIN(last_trip_duration_minutes) AS last_trip_duration_minutes,
  MAX(duration_minutes) AS highest_trip_duration_minutes,
  MIN(duration_minutes) AS lowest_trip_duration_minutes
FROM windowed_data
GROUP BY year_month
ORDER BY year_month