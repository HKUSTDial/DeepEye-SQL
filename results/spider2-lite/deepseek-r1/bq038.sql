WITH round_trips AS (
  SELECT 
    bikeid,
    start_station_id,
    end_station_id,
    end_station_name,
    starttime,
    stoptime
  FROM `bigquery-public-data.new_york.citibike_trips`
  WHERE start_station_id = end_station_id
),
group_flagged AS (
  SELECT 
    rt1.end_station_id,
    rt1.end_station_name,
    CASE WHEN EXISTS (
      SELECT 1
      FROM round_trips rt2
      WHERE rt1.start_station_id = rt2.start_station_id
        AND rt1.bikeid != rt2.bikeid
        AND ABS(TIMESTAMP_DIFF(rt1.starttime, rt2.starttime, SECOND)) <= 120
        AND ABS(TIMESTAMP_DIFF(rt1.stoptime, rt2.stoptime, SECOND)) <= 120
    ) THEN 1 ELSE 0 END AS is_group_ride
  FROM round_trips rt1
),
group_rides_per_station AS (
  SELECT 
    end_station_id,
    end_station_name,
    SUM(is_group_ride) AS group_rides_count
  FROM group_flagged
  GROUP BY end_station_id, end_station_name
),
total_trips_per_station AS (
  SELECT 
    end_station_id,
    COUNT(*) AS total_trips_count
  FROM `bigquery-public-data.new_york.citibike_trips`
  GROUP BY end_station_id
)
SELECT 
  t.end_station_id AS station_id,
  g.end_station_name AS station_name,
  t.total_trips_count,
  COALESCE(g.group_rides_count, 0) AS group_rides,
  SAFE_DIVIDE(COALESCE(g.group_rides_count, 0), t.total_trips_count) AS proportion
FROM total_trips_per_station t
LEFT JOIN group_rides_per_station g ON t.end_station_id = g.end_station_id
ORDER BY proportion DESC
LIMIT 10