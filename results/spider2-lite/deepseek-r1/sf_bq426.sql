WITH zip_geom AS (
    SELECT TO_GEOGRAPHY("zip_code_geom") AS geom
    FROM "NEW_YORK_CITIBIKE_1"."GEO_US_BOUNDARIES"."ZIP_CODES"
    WHERE "zip_code" = '10019'
),
stations_in_zip AS (
    SELECT "station_id"
    FROM "NEW_YORK_CITIBIKE_1"."NEW_YORK_CITIBIKE"."CITIBIKE_STATIONS" s
    CROSS JOIN zip_geom
    WHERE ST_WITHIN(ST_MAKEPOINT(s."longitude", s."latitude"), zip_geom.geom)
),
weather_data AS (
    SELECT 
        DATE_FROM_PARTS(CAST(gsod."year" AS INTEGER), CAST(gsod."mo" AS INTEGER), CAST(gsod."da" AS INTEGER)) AS weather_date,
        gsod."temp",
        gsod."prcp",
        gsod."wdsp"
    FROM "NEW_YORK_CITIBIKE_1"."NOAA_GSOD"."GSOD2018" gsod
    JOIN "NEW_YORK_CITIBIKE_1"."NOAA_GSOD"."STATIONS" stations
        ON gsod."stn" = stations."usaf" AND gsod."wban" = stations."wban"
    WHERE UPPER(stations."name") LIKE '%CENTRAL PARK%'
        AND gsod."temp" != 9999.9
        AND gsod."prcp" != 99.99
        AND gsod."wdsp" != '999.9'
),
trips_in_zip AS (
    SELECT 
        trips."usertype",
        DATE(TO_TIMESTAMP(trips."starttime" / 1000000)) AS trip_date
    FROM "NEW_YORK_CITIBIKE_1"."NEW_YORK_CITIBIKE"."CITIBIKE_TRIPS" trips
    WHERE EXTRACT(YEAR FROM TO_TIMESTAMP(trips."starttime" / 1000000)) = 2018
        AND EXISTS (SELECT 1 FROM stations_in_zip WHERE stations_in_zip."station_id" = CAST(trips."start_station_id" AS TEXT))
        AND EXISTS (SELECT 1 FROM stations_in_zip WHERE stations_in_zip."station_id" = CAST(trips."end_station_id" AS TEXT))
)
SELECT 
    trips."usertype",
    AVG(w."temp") AS avg_temp,
    AVG(w."prcp") AS avg_prcp,
    AVG(CAST(w."wdsp" AS FLOAT)) AS avg_wind_speed
FROM trips_in_zip trips
JOIN weather_data w ON trips.trip_date = w.weather_date
GROUP BY trips."usertype"
ORDER BY avg_temp DESC
LIMIT 1