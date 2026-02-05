WITH trip_data AS (
  SELECT 
    TO_TIMESTAMP("start_date" / 1000000) AS start_timestamp,
    "duration_sec" / 60.0 AS duration_minutes,
    "trip_id",
    TO_CHAR(TO_TIMESTAMP("start_date" / 1000000), 'YYYYMM') AS year_month
  FROM "SAN_FRANCISCO"."SAN_FRANCISCO"."BIKESHARE_TRIPS"
),
windowed AS (
  SELECT 
    year_month,
    duration_minutes,
    ROW_NUMBER() OVER (PARTITION BY year_month ORDER BY start_timestamp ASC, "trip_id" ASC) AS rn_asc,
    ROW_NUMBER() OVER (PARTITION BY year_month ORDER BY start_timestamp DESC, "trip_id" DESC) AS rn_desc
  FROM trip_data
)
SELECT 
  year_month,
  MAX(CASE WHEN rn_asc = 1 THEN duration_minutes END) AS first_duration_minutes,
  MAX(CASE WHEN rn_desc = 1 THEN duration_minutes END) AS last_duration_minutes,
  MAX(duration_minutes) AS highest_duration_minutes,
  MIN(duration_minutes) AS lowest_duration_minutes
FROM windowed
GROUP BY year_month
ORDER BY year_month