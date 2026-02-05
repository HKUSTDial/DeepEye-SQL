WITH zip_geom AS (
    SELECT TO_GEOGRAPHY("zip_code_geom") AS geom
    FROM "NEW_YORK_CITIBIKE_1"."GEO_US_BOUNDARIES"."ZIP_CODES"
    WHERE "zip_code" = '10019'
),
central_park_station AS (
    SELECT "usaf", "wban"
    FROM "NEW_YORK_CITIBIKE_1"."NOAA_GSOD"."STATIONS"
    WHERE UPPER("name") LIKE '%CENTRAL PARK%'
),
weather_2018 AS (
    SELECT 
        DATE_FROM_PARTS("year"::INTEGER, "mo"::INTEGER, "da"::INTEGER) AS weather_date,
        "temp",
        "prcp",
        "wdsp"
    FROM "NEW_YORK_CITIBIKE_1"."NOAA_GSOD"."GSOD2018" w
    INNER JOIN central_park_station cps ON w."stn" = cps."usaf" AND w."wban" = cps."wban"
    WHERE "temp" != 9999.9 AND "prcp" != 99.99 AND "wdsp" != 999.9
),
trips_2018_10019 AS (
    SELECT 
        "usertype",
        DATE(TO_TIMESTAMP("starttime" / 1000000)) AS trip_date
    FROM "NEW_YORK_CITIBIKE_1"."NEW_YORK_CITIBIKE"."CITIBIKE_TRIPS" t, zip_geom z
    WHERE EXTRACT(YEAR FROM TO_TIMESTAMP("starttime" / 1000000)) = 2018
      AND ST_WITHIN(ST_MAKEPOINT("start_station_longitude", "start_station_latitude"), z.geom)
      AND ST_WITHIN(ST_MAKEPOINT("end_station_longitude", "end_station_latitude"), z.geom)
),
joined_data AS (
    SELECT 
        t."usertype",
        w."temp",
        w."prcp",
        w."wdsp"
    FROM trips_2018_10019 t
    INNER JOIN weather_2018 w ON t.trip_date = w.weather_date
),
aggregated AS (
    SELECT 
        "usertype",
        AVG("temp") AS avg_temp,
        AVG("prcp") AS avg_prcp,
        AVG("wdsp") AS avg_wdsp
    FROM joined_data
    GROUP BY "usertype"
)
SELECT 
    "usertype",
    avg_temp,
    avg_prcp,
    avg_wdsp
FROM aggregated
ORDER BY avg_temp DESC
LIMIT 1