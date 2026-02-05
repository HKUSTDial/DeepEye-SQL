WITH all_combinations AS (
  SELECT 
    z.zip_code,
    hour
  FROM `bigquery-public-data.geo_us_boundaries.zip_codes` z
  CROSS JOIN UNNEST(GENERATE_ARRAY(0, 23)) AS hour
  WHERE z.state_name = 'New York'
),
current_day_trips AS (
  SELECT 
    EXTRACT(HOUR FROM t.pickup_datetime) AS hour,
    z.zip_code,
    COUNT(*) AS trip_count
  FROM `bigquery-public-data.new_york.tlc_yellow_trips_2015` t
  JOIN `bigquery-public-data.geo_us_boundaries.zip_codes` z
    ON ST_CONTAINS(z.zip_code_geom, 
                  ST_GEOGPOINT(t.pickup_longitude, t.pickup_latitude))
  WHERE DATE(t.pickup_datetime) = '2015-01-01'
    AND t.pickup_latitude IS NOT NULL
    AND t.pickup_longitude IS NOT NULL
    AND z.state_name = 'New York'
  GROUP BY hour, zip_code
),
historical_trips AS (
  SELECT 
    EXTRACT(HOUR FROM t.pickup_datetime) AS hour,
    z.zip_code,
    DATE(t.pickup_datetime) AS trip_date,
    COUNT(*) AS trip_count
  FROM `bigquery-public-data.new_york.tlc_yellow_trips_2014` t
  JOIN `bigquery-public-data.geo_us_boundaries.zip_codes` z
    ON ST_CONTAINS(z.zip_code_geom, 
                  ST_GEOGPOINT(t.pickup_longitude, t.pickup_latitude))
  WHERE DATE(t.pickup_datetime) BETWEEN '2014-12-11' AND '2014-12-31'
    AND t.pickup_latitude IS NOT NULL
    AND t.pickup_longitude IS NOT NULL
    AND z.state_name = 'New York'
  GROUP BY hour, zip_code, trip_date
),
trip_metrics AS (
  SELECT 
    ac.zip_code,
    ac.hour,
    COALESCE(cdt.trip_count, 0) AS current_hour_trips,
    COALESCE(
      CASE 
        WHEN ac.hour = 0 THEN h0.trip_count
        ELSE h1.trip_count
      END, 0) AS trips_1_hour_ago,
    COALESCE(h24.trip_count, 0) AS trips_1_day_ago,
    COALESCE(h168.trip_count, 0) AS trips_7_days_ago,
    COALESCE(h336.trip_count, 0) AS trips_14_days_ago
  FROM all_combinations ac
  LEFT JOIN current_day_trips cdt 
    ON ac.zip_code = cdt.zip_code AND ac.hour = cdt.hour
  LEFT JOIN current_day_trips h1 
    ON ac.zip_code = h1.zip_code AND ac.hour = h1.hour + 1 AND ac.hour > 0
  LEFT JOIN historical_trips h0 
    ON ac.zip_code = h0.zip_code AND ac.hour = 0 AND h0.hour = 23 AND h0.trip_date = '2014-12-31'
  LEFT JOIN historical_trips h24 
    ON ac.zip_code = h24.zip_code AND ac.hour = h24.hour AND h24.trip_date = '2014-12-31'
  LEFT JOIN historical_trips h168 
    ON ac.zip_code = h168.zip_code AND ac.hour = h168.hour AND h168.trip_date = '2014-12-25'
  LEFT JOIN historical_trips h336 
    ON ac.zip_code = h336.zip_code AND ac.hour = h336.hour AND h336.trip_date = '2014-12-18'
),
moving_stats AS (
  SELECT 
    ac.zip_code,
    ac.hour,
    AVG(COALESCE(ht.trip_count, 0)) AS moving_avg_14d,
    STDDEV_SAMP(COALESCE(ht.trip_count, 0)) AS moving_stddev_14d,
    AVG(COALESCE(ht2.trip_count, 0)) AS moving_avg_21d,
    STDDEV_SAMP(COALESCE(ht2.trip_count, 0)) AS moving_stddev_21d
  FROM all_combinations ac
  LEFT JOIN historical_trips ht 
    ON ac.zip_code = ht.zip_code AND ac.hour = ht.hour 
    AND ht.trip_date BETWEEN '2014-12-18' AND '2014-12-31'
  LEFT JOIN historical_trips ht2 
    ON ac.zip_code = ht2.zip_code AND ac.hour = ht2.hour 
    AND ht2.trip_date BETWEEN '2014-12-11' AND '2014-12-31'
  GROUP BY ac.zip_code, ac.hour
)
SELECT 
  tm.zip_code,
  tm.hour,
  tm.current_hour_trips,
  tm.trips_1_hour_ago,
  tm.trips_1_day_ago,
  tm.trips_7_days_ago,
  tm.trips_14_days_ago,
  ms.moving_avg_14d,
  ms.moving_stddev_14d,
  ms.moving_avg_21d,
  ms.moving_stddev_21d
FROM trip_metrics tm
JOIN moving_stats ms 
  ON tm.zip_code = ms.zip_code AND tm.hour = ms.hour
ORDER BY tm.current_hour_trips DESC
LIMIT 5