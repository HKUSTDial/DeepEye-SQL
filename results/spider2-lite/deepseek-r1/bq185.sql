SELECT AVG(TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, MINUTE)) AS avg_duration_minutes
FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2016` AS trips
INNER JOIN `bigquery-public-data.new_york_taxi_trips.taxi_zone_geom` AS pickup_zone
  ON trips.pickup_location_id = pickup_zone.zone_id
INNER JOIN `bigquery-public-data.new_york_taxi_trips.taxi_zone_geom` AS dropoff_zone
  ON trips.dropoff_location_id = dropoff_zone.zone_id
WHERE DATE(pickup_datetime) BETWEEN '2016-02-01' AND '2016-02-07'
  AND dropoff_datetime > pickup_datetime
  AND passenger_count > 3
  AND trip_distance >= 10
  AND pickup_zone.borough = 'Brooklyn'
  AND dropoff_zone.borough = 'Brooklyn'