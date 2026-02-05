WITH central_park_station AS (
  SELECT "usaf", "wban" FROM "NEW_YORK_CITIBIKE_1"."NOAA_GSOD"."STATIONS" WHERE "name" LIKE '%CENTRAL PARK%' LIMIT 1
),
weather_2014 AS (
  SELECT DATE_FROM_PARTS(CAST("year" AS INTEGER), CAST("mo" AS INTEGER), CAST("da" AS INTEGER)) AS weather_date,
         "temp", "wdsp", "prcp"
  FROM "NEW_YORK_CITIBIKE_1"."NOAA_GSOD"."GSOD2014" g
  INNER JOIN central_park_station cps ON g."stn" = cps."usaf" AND g."wban" = cps."wban"
  WHERE g."temp" < 9999.9 AND g."wdsp" < 999.9 AND g."prcp" < 99.99
),
trips_2014 AS (
  SELECT *,
         DATE(TO_TIMESTAMP("starttime" / 1000000)) AS trip_date,
         EXTRACT(YEAR FROM trip_date) AS trip_year,
         EXTRACT(MONTH FROM trip_date) AS trip_month,
         ROW_NUMBER() OVER (ORDER BY "starttime") AS trip_id
  FROM "NEW_YORK_CITIBIKE_1"."NEW_YORK_CITIBIKE"."CITIBIKE_TRIPS"
  WHERE trip_year = 2014
),
start_zip AS (
  SELECT t.*,
         zc."zip_code" AS start_zip_code
  FROM trips_2014 t
  LEFT JOIN "NEW_YORK_CITIBIKE_1"."GEO_US_BOUNDARIES"."ZIP_CODES" zc
    ON ST_WITHIN(ST_POINT(t."start_station_longitude", t."start_station_latitude"), zc."zip_code_geom")
  QUALIFY ROW_NUMBER() OVER (PARTITION BY t.trip_id ORDER BY zc."zip_code") = 1
),
start_neigh AS (
  SELECT sz.*,
         cz."neighborhood" AS start_neighborhood,
         cz."borough" AS start_borough
  FROM start_zip sz
  LEFT JOIN "NEW_YORK_CITIBIKE_1"."CYCLISTIC"."ZIP_CODES" cz ON sz.start_zip_code = CAST(cz."zip" AS TEXT)
),
end_zip AS (
  SELECT sn.*,
         zc."zip_code" AS end_zip_code
  FROM start_neigh sn
  LEFT JOIN "NEW_YORK_CITIBIKE_1"."GEO_US_BOUNDARIES"."ZIP_CODES" zc
    ON ST_WITHIN(ST_POINT(sn."end_station_longitude", sn."end_station_latitude"), zc."zip_code_geom")
  QUALIFY ROW_NUMBER() OVER (PARTITION BY sn.trip_id ORDER BY zc."zip_code") = 1
),
end_neigh AS (
  SELECT ez.*,
         cz."neighborhood" AS end_neighborhood,
         cz."borough" AS end_borough
  FROM end_zip ez
  LEFT JOIN "NEW_YORK_CITIBIKE_1"."CYCLISTIC"."ZIP_CODES" cz ON ez.end_zip_code = CAST(cz."zip" AS TEXT)
),
trips_with_weather AS (
  SELECT en.*,
         w."temp", w."wdsp", w."prcp"
  FROM end_neigh en
  INNER JOIN weather_2014 w ON en.trip_date = w.weather_date
  WHERE en.start_neighborhood IS NOT NULL AND en.end_neighborhood IS NOT NULL
)
SELECT start_neighborhood,
       end_neighborhood,
       COUNT(*) AS total_trips,
       ROUND(AVG("tripduration") / 60.0, 1) AS avg_duration_minutes,
       ROUND(AVG("temp"), 1) AS avg_temperature_f,
       ROUND(AVG("wdsp") * 0.514444, 1) AS avg_wind_speed_mps,
       ROUND(AVG("prcp") * 2.54, 1) AS avg_precipitation_cm,
       MODE("trip_month") AS month_with_most_trips
FROM trips_with_weather
GROUP BY start_neighborhood, end_neighborhood
ORDER BY start_neighborhood, end_neighborhood