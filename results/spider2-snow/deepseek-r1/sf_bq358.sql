WITH central_park_temp AS (
    SELECT "temp" 
    FROM "NEW_YORK_CITIBIKE_1"."NOAA_GSOD"."GSOD2015" 
    WHERE "wban" = '94728' 
      AND "year" = '2015' 
      AND "mo" = '07' 
      AND "da" = '15'
),
stations_with_same_temp AS (
    SELECT gs."stn", gs."wban", s."lat", s."lon"
    FROM "NEW_YORK_CITIBIKE_1"."NOAA_GSOD"."GSOD2015" gs
    JOIN "NEW_YORK_CITIBIKE_1"."NOAA_GSOD"."STATIONS" s ON gs."wban" = s."wban"
    WHERE gs."year" = '2015' 
      AND gs."mo" = '07' 
      AND gs."da" = '15'
      AND gs."temp" = (SELECT "temp" FROM central_park_temp)
),
zip_codes_with_temp AS (
    SELECT DISTINCT z."zip_code", 
           (z."area_land_meters" + COALESCE(z."area_water_meters", 0)) as "total_area"
    FROM "NEW_YORK_CITIBIKE_1"."GEO_US_BOUNDARIES"."ZIP_CODES" z
    JOIN stations_with_same_temp st ON ST_WITHIN(ST_POINT(st."lon", st."lat"), z."zip_code_geom")
),
matching_trips AS (
    SELECT t."bikeid",
           start_z."zip_code" as "start_zip",
           end_z."zip_code" as "end_zip",
           start_z."total_area" as "start_zip_area",
           end_z."total_area" as "end_zip_area"
    FROM "NEW_YORK_CITIBIKE_1"."NEW_YORK_CITIBIKE"."CITIBIKE_TRIPS" t
    JOIN zip_codes_with_temp start_z ON ST_WITHIN(ST_POINT(t."start_station_longitude", t."start_station_latitude"), 
                                                  (SELECT "zip_code_geom" FROM "NEW_YORK_CITIBIKE_1"."GEO_US_BOUNDARIES"."ZIP_CODES" WHERE "zip_code" = start_z."zip_code"))
    JOIN zip_codes_with_temp end_z ON ST_WITHIN(ST_POINT(t."end_station_longitude", t."end_station_latitude"),
                                                (SELECT "zip_code_geom" FROM "NEW_YORK_CITIBIKE_1"."GEO_US_BOUNDARIES"."ZIP_CODES" WHERE "zip_code" = end_z."zip_code"))
    WHERE DATE(TO_TIMESTAMP(t."starttime" / 1000000)) = '2015-07-15'
)
SELECT "start_zip", "end_zip"
FROM matching_trips
ORDER BY "start_zip_area" ASC, "end_zip_area" DESC
LIMIT 1