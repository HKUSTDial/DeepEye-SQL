WITH trips_2018 AS (
  SELECT 
    company,
    trip_start_timestamp
  FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
  WHERE trip_start_timestamp >= '2018-01-01' 
    AND trip_start_timestamp < '2019-01-01'
    AND company IS NOT NULL
),
monthly_counts AS (
  SELECT 
    company,
    DATE(DATE_TRUNC(trip_start_timestamp, MONTH)) AS month,
    COUNT(*) AS trip_count
  FROM trips_2018
  GROUP BY company, month
),
all_months AS (
  SELECT month
  FROM UNNEST(GENERATE_DATE_ARRAY('2018-01-01', '2018-12-01', INTERVAL 1 MONTH)) AS month
),
distinct_companies AS (
  SELECT DISTINCT company
  FROM trips_2018
),
all_combinations AS (
  SELECT 
    am.month,
    dc.company
  FROM all_months am
  CROSS JOIN distinct_companies dc
),
full_counts AS (
  SELECT 
    ac.month,
    ac.company,
    IFNULL(mc.trip_count, 0) AS trip_count
  FROM all_combinations ac
  LEFT JOIN monthly_counts mc 
    ON ac.company = mc.company 
    AND ac.month = mc.month
),
with_lag AS (
  SELECT 
    company,
    month,
    trip_count,
    LAG(trip_count) OVER (PARTITION BY company ORDER BY month) AS prev_trip_count,
    trip_count - LAG(trip_count) OVER (PARTITION BY company ORDER BY month) AS increase
  FROM full_counts
),
increases AS (
  SELECT 
    company,
    increase
  FROM with_lag
  WHERE increase IS NOT NULL 
    AND increase > 0
),
max_increases AS (
  SELECT 
    company,
    MAX(increase) AS max_increase
  FROM increases
  GROUP BY company
)
SELECT 
  company
FROM max_increases
ORDER BY max_increase DESC
LIMIT 3