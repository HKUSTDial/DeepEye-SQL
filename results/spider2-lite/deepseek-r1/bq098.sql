WITH filtered_trips AS (
  SELECT 
    t.*,
    IF(t.total_amount = 0, 0, (t.tip_amount * 100) / t.total_amount) AS tip_rate
  FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2016` AS t
  WHERE 
    DATE(t.pickup_datetime) BETWEEN '2016-01-01' AND '2016-01-07'
    AND DATE(t.dropoff_datetime) BETWEEN '2016-01-01' AND '2016-01-07'
    AND t.dropoff_datetime > t.pickup_datetime
    AND t.passenger_count > 0
    AND t.trip_distance >= 0
    AND t.tip_amount >= 0
    AND t.tolls_amount >= 0
    AND t.mta_tax >= 0
    AND t.fare_amount >= 0
    AND t.total_amount >= 0
),
trips_with_borough AS (
  SELECT 
    ft.*,
    tzg.borough AS pickup_borough
  FROM filtered_trips AS ft
  JOIN `bigquery-public-data.new_york_taxi_trips.taxi_zone_geom` AS tzg
    ON ft.pickup_location_id = tzg.zone_id
)
SELECT 
  pickup_borough,
  (SUM(CASE WHEN tip_rate = 0 THEN 1 ELSE 0 END) * 100.0) / COUNT(*) AS no_tip_percentage
FROM trips_with_borough
GROUP BY pickup_borough