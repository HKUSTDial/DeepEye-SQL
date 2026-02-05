WITH bike_routes_2016 AS (
  SELECT
    ROUND("start_station_latitude", 3) AS start_lat,
    ROUND("start_station_longitude", 3) AS start_lon,
    ROUND("end_station_latitude", 3) AS end_lat,
    ROUND("end_station_longitude", 3) AS end_lon,
    MIN("start_station_name") AS start_station_name,
    COUNT(*) AS trip_count,
    AVG("tripduration") AS avg_bike_duration
  FROM "NEW_YORK"."NEW_YORK"."CITIBIKE_TRIPS"
  WHERE EXTRACT(YEAR FROM TO_TIMESTAMP("starttime" / 1000000)) = 2016
    AND "start_station_latitude" IS NOT NULL
    AND "start_station_longitude" IS NOT NULL
    AND "end_station_latitude" IS NOT NULL
    AND "end_station_longitude" IS NOT NULL
  GROUP BY 1, 2, 3, 4
  ORDER BY trip_count DESC
  LIMIT 20
),
taxi_routes_2016 AS (
  SELECT
    ROUND("pickup_latitude", 3) AS pickup_lat,
    ROUND("pickup_longitude", 3) AS pickup_lon,
    ROUND("dropoff_latitude", 3) AS dropoff_lat,
    ROUND("dropoff_longitude", 3) AS dropoff_lon,
    AVG(("dropoff_datetime" - "pickup_datetime") / 1000000) AS avg_taxi_duration
  FROM "NEW_YORK"."NEW_YORK"."TLC_YELLOW_TRIPS_2016"
  WHERE EXTRACT(YEAR FROM TO_TIMESTAMP("pickup_datetime" / 1000000)) = 2016
    AND "pickup_latitude" IS NOT NULL
    AND "pickup_longitude" IS NOT NULL
    AND "dropoff_latitude" IS NOT NULL
    AND "dropoff_longitude" IS NOT NULL
    AND "pickup_latitude" != 'nan'
    AND "pickup_longitude" != 'nan'
    AND "dropoff_latitude" != 'nan'
    AND "dropoff_longitude" != 'nan'
  GROUP BY 1, 2, 3, 4
)
SELECT bike_routes_2016.start_station_name
FROM bike_routes_2016
INNER JOIN taxi_routes_2016
  ON bike_routes_2016.start_lat = taxi_routes_2016.pickup_lat
  AND bike_routes_2016.start_lon = taxi_routes_2016.pickup_lon
  AND bike_routes_2016.end_lat = taxi_routes_2016.dropoff_lat
  AND bike_routes_2016.end_lon = taxi_routes_2016.dropoff_lon
WHERE bike_routes_2016.avg_bike_duration < taxi_routes_2016.avg_taxi_duration
ORDER BY bike_routes_2016.avg_bike_duration DESC
LIMIT 1