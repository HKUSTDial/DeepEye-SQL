WITH filtered_trips AS (
  SELECT 
    pickup_location_id,
    dropoff_location_id,
    pickup_datetime,
    dropoff_datetime,
    trip_distance,
    tip_amount,
    total_amount
  FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2016`
  WHERE DATE(pickup_datetime) BETWEEN '2016-07-01' AND '2016-07-07'
    AND DATE(dropoff_datetime) BETWEEN '2016-07-01' AND '2016-07-07'
    AND passenger_count > 5
    AND trip_distance >= 10
    AND tip_amount >= 0
    AND tolls_amount >= 0
    AND mta_tax >= 0
    AND fare_amount >= 0
    AND total_amount >= 0
    AND dropoff_datetime > pickup_datetime
  UNION ALL
  SELECT 
    pickup_location_id,
    dropoff_location_id,
    pickup_datetime,
    dropoff_datetime,
    trip_distance,
    tip_amount,
    total_amount
  FROM `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2016`
  WHERE DATE(pickup_datetime) BETWEEN '2016-07-01' AND '2016-07-07'
    AND DATE(dropoff_datetime) BETWEEN '2016-07-01' AND '2016-07-07'
    AND passenger_count > 5
    AND trip_distance >= 10
    AND tip_amount >= 0
    AND tolls_amount >= 0
    AND mta_tax >= 0
    AND fare_amount >= 0
    AND total_amount >= 0
    AND dropoff_datetime > pickup_datetime
)
SELECT 
  pz.zone_name AS pickup_zone,
  dz.zone_name AS dropoff_zone,
  TIMESTAMP_DIFF(t.dropoff_datetime, t.pickup_datetime, SECOND) AS trip_duration_seconds,
  t.trip_distance * 3600 / NULLIF(TIMESTAMP_DIFF(t.dropoff_datetime, t.pickup_datetime, SECOND), 0) AS driving_speed_mph,
  (t.tip_amount / NULLIF(t.total_amount, 0)) * 100 AS tip_rate_percentage
FROM filtered_trips t
LEFT JOIN `bigquery-public-data.new_york_taxi_trips.taxi_zone_geom` pz
  ON t.pickup_location_id = pz.zone_id
LEFT JOIN `bigquery-public-data.new_york_taxi_trips.taxi_zone_geom` dz
  ON t.dropoff_location_id = dz.zone_id
ORDER BY t.total_amount DESC
LIMIT 10