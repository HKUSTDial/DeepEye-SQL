WITH trips_2014 AS (
   SELECT 
      "bikeid",
      "tripduration",
      "start_station_latitude",
      "start_station_longitude",
      "end_station_latitude",
      "end_station_longitude",
      "starttime",
      DATE(CONVERT_TIMEZONE('UTC', 'America/New_York', TO_TIMESTAMP("starttime"/1000000))) AS "start_date"
   FROM "NEW_YORK_CITIBIKE_1"."NEW_YORK_CITIBIKE"."CITIBIKE_TRIPS"
   WHERE YEAR("start_date") = 2014
),
start_zip AS (
   SELECT t.*, gz."zip_code" AS "start_zip_code"
   FROM trips_2014 t
   JOIN "NEW_YORK_CITIBIKE_1"."GEO_US_BOUNDARIES"."ZIP_CODES" gz
      ON ST_WITHIN(ST_MAKEPOINT(t."start_station_longitude", t."start_station_latitude"), TO_GEOGRAPHY(gz."zip_code_geom"))
   WHERE gz."state_code" = 'NY'
),
end_zip AS (
   SELECT sz.*, gz."zip_code" AS "end_zip_code"
   FROM start_zip sz
   JOIN "NEW_YORK_CITIBIKE_1"."GEO_US_BOUNDARIES"."ZIP_CODES" gz
      ON ST_WITHIN(ST_MAKEPOINT(sz."end_station_longitude", sz."end_station_latitude"), TO_GEOGRAPHY(gz."zip_code_geom"))
   WHERE gz."state_code" = 'NY'
),
start_neigh AS (
   SELECT ez.*, cz."neighborhood" AS "start_neighborhood", cz."borough" AS "start_borough"
   FROM end_zip ez
   JOIN "NEW_YORK_CITIBIKE_1"."CYCLISTIC"."ZIP_CODES" cz
      ON ez."start_zip_code" = CAST(cz."zip" AS TEXT)
),
end_neigh AS (
   SELECT sn.*, cz."neighborhood" AS "end_neighborhood", cz."borough" AS "end_borough"
   FROM start_neigh sn
   JOIN "NEW_YORK_CITIBIKE_1"."CYCLISTIC"."ZIP_CODES" cz
      ON sn."end_zip_code" = CAST(cz."zip" AS TEXT)
),
weather AS (
   SELECT 
      g."stn",
      g."wban",
      TO_DATE(g."year" || '-' || g."mo" || '-' || g."da", 'YYYY-MM-DD') AS "weather_date",
      CASE WHEN g."temp" != 9999.9 THEN g."temp" ELSE NULL END AS "temp",
      CASE WHEN g."wdsp" != '999.9' THEN g."wdsp"::FLOAT ELSE NULL END AS "wdsp",
      CASE WHEN g."prcp" != 99.99 THEN g."prcp" ELSE NULL END AS "prcp"
   FROM "NEW_YORK_CITIBIKE_1"."NOAA_GSOD"."GSOD2014" g
   JOIN "NEW_YORK_CITIBIKE_1"."NOAA_GSOD"."STATIONS" s
      ON g."stn" = s."usaf" AND g."wban" = s."wban"
   WHERE s."name" ILIKE '%CENTRAL PARK%'
),
trips_with_weather AS (
   SELECT 
      tn.*,
      w."temp",
      w."wdsp",
      w."prcp",
      MONTH(tn."start_date") AS "start_month"
   FROM end_neigh tn
   LEFT JOIN weather w
      ON tn."start_date" = w."weather_date"
),
monthly_counts AS (
   SELECT
      "start_neighborhood",
      "end_neighborhood",
      "start_month",
      COUNT(*) AS "month_trips"
   FROM trips_with_weather
   GROUP BY "start_neighborhood", "end_neighborhood", "start_month"
),
ranked_months AS (
   SELECT *,
      ROW_NUMBER() OVER (PARTITION BY "start_neighborhood", "end_neighborhood" ORDER BY "month_trips" DESC, "start_month" ASC) AS "rn"
   FROM monthly_counts
),
mode_month AS (
   SELECT "start_neighborhood", "end_neighborhood", "start_month" AS "mode_month"
   FROM ranked_months
   WHERE "rn" = 1
)
SELECT
   tww."start_neighborhood",
   tww."end_neighborhood",
   COUNT(*) AS "total_trips",
   ROUND(AVG(tww."tripduration")/60, 1) AS "avg_duration_minutes",
   ROUND(AVG(tww."temp"), 1) AS "avg_temp",
   ROUND(AVG(tww."wdsp" * 0.514444), 1) AS "avg_wind_speed_mps",
   ROUND(AVG(tww."prcp" * 2.54), 1) AS "avg_precipitation_cm",
   mm."mode_month" AS "month_with_most_trips"
FROM trips_with_weather tww
JOIN mode_month mm
   ON tww."start_neighborhood" = mm."start_neighborhood" AND tww."end_neighborhood" = mm."end_neighborhood"
GROUP BY tww."start_neighborhood", tww."end_neighborhood", mm."mode_month"
ORDER BY "total_trips" DESC