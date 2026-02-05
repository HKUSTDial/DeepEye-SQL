WITH bike_trips_2016 AS (
  SELECT 
    ROUND(start_station_latitude, 3) AS start_lat,
    ROUND(start_station_longitude, 3) AS start_lon,
    ROUND(end_station_latitude, 3) AS end_lat,
    ROUND(end_station_longitude, 3) AS end_lon,
    start_station_name,
    tripduration
  FROM `bigquery-public-data.new_york.citibike_trips`
  WHERE EXTRACT(YEAR FROM starttime) = 2016
),
route_aggregates AS (
  SELECT 
    start_lat,
    start_lon,
    end_lat,
    end_lon,
    COUNT(*) AS trip_count,
    AVG(tripduration) AS avg_bike_duration
  FROM bike_trips_2016
  GROUP BY start_lat, start_lon, end_lat, end_lon
),
route_mode AS (
  SELECT 
    start_lat,
    start_lon,
    end_lat,
    end_lon,
    ARRAY_AGG(start_station_name ORDER BY cnt DESC)[OFFSET(0)] AS start_station_name
  FROM (
    SELECT 
      start_lat,
      start_lon,
      end_lat,
      end_lon,
      start_station_name,
      COUNT(*) AS cnt
    FROM bike_trips_2016
    GROUP BY start_lat, start_lon, end_lat, end_lon, start_station_name
  )
  GROUP BY start_lat, start_lon, end_lat, end_lon
),
route_with_station AS (
  SELECT 
    ra.start_lat,
    ra.start_lon,
    ra.end_lat,
    ra.end_lon,
    ra.trip_count,
    ra.avg_bike_duration,
    rm.start_station_name
  FROM route_aggregates ra
  JOIN route_mode rm 
    ON ra.start_lat = rm.start_lat 
    AND ra.start_lon = rm.start_lon 
    AND ra.end_lat = rm.end_lat 
    AND ra.end_lon = rm.end_lon
),
top_20_bike_routes AS (
  SELECT 
    start_lat,
    start_lon,
    end_lat,
    end_lon,
    trip_count,
    avg_bike_duration,
    start_station_name
  FROM route_with_station
  ORDER BY trip_count DESC
  LIMIT 20
),
taxi_trips_2016 AS (
  SELECT 
    ROUND(pickup_latitude, 3) AS pickup_lat,
    ROUND(pickup_longitude, 3) AS pickup_lon,
    ROUND(dropoff_latitude, 3) AS dropoff_lat,
    ROUND(dropoff_longitude, 3) AS dropoff_lon,
    TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS taxi_duration
  FROM `bigquery-public-data.new_york.tlc_yellow_trips_2016`
  WHERE EXTRACT(YEAR FROM pickup_datetime) = 2016
    AND pickup_latitude IS NOT NULL 
    AND pickup_longitude IS NOT NULL 
    AND dropoff_latitude IS NOT NULL 
    AND dropoff_longitude IS NOT NULL
    AND pickup_latitude != 0 
    AND pickup_longitude != 0 
    AND dropoff_latitude != 0 
    AND dropoff_longitude != 0
),
taxi_route_avg AS (
  SELECT 
    pickup_lat,
    pickup_lon,
    dropoff_lat,
    dropoff_lon,
    AVG(taxi_duration) AS avg_taxi_duration
  FROM taxi_trips_2016
  GROUP BY pickup_lat, pickup_lon, dropoff_lat, dropoff_lon
),
faster_routes AS (
  SELECT 
    t20.start_lat,
    t20.start_lon,
    t20.end_lat,
    t20.end_lon,
    t20.avg_bike_duration,
    t20.start_station_name,
    tra.avg_taxi_duration
  FROM top_20_bike_routes t20
  INNER JOIN taxi_route_avg tra 
    ON t20.start_lat = tra.pickup_lat 
    AND t20.start_lon = tra.pickup_lon 
    AND t20.end_lat = tra.dropoff_lat 
    AND t20.end_lon = tra.dropoff_lon
  WHERE t20.avg_bike_duration < tra.avg_taxi_duration
)
SELECT 
  start_station_name
FROM faster_routes
ORDER BY avg_bike_duration DESC
LIMIT 1