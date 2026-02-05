WITH trips_2018 AS (
  SELECT "start_station_id", COUNT(*) AS trip_count
  FROM "NEW_YORK_PLUS"."NEW_YORK_CITIBIKE"."CITIBIKE_TRIPS"
  WHERE EXTRACT(YEAR FROM TO_TIMESTAMP("starttime" / 1000000)) = 2018
  GROUP BY "start_station_id"
), top_station AS (
  SELECT "start_station_id"
  FROM trips_2018
  ORDER BY trip_count DESC
  LIMIT 1
), station_hourly AS (
  SELECT EXTRACT(DAYOFWEEK FROM TO_TIMESTAMP("starttime" / 1000000)) AS day_of_week, EXTRACT(HOUR FROM TO_TIMESTAMP("starttime" / 1000000)) AS hour_of_day, COUNT(*) AS trip_count
  FROM "NEW_YORK_PLUS"."NEW_YORK_CITIBIKE"."CITIBIKE_TRIPS"
  WHERE EXTRACT(YEAR FROM TO_TIMESTAMP("starttime" / 1000000)) = 2018 AND "start_station_id" = (SELECT "start_station_id" FROM top_station)
  GROUP BY day_of_week, hour_of_day
)
SELECT day_of_week, hour_of_day
FROM station_hourly
ORDER BY trip_count DESC
LIMIT 1