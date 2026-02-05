WITH highest_temp AS (
    SELECT MAX("temp") as max_temp
    FROM "NEW_YORK_CITIBIKE_1"."NOAA_GSOD"."GSOD2015"
    WHERE "year" = '2015' AND "mo" = '07' AND "da" = '15' AND "wban" = '94728'
),
hot_zip_codes AS (
    SELECT DISTINCT z."zip_code"
    FROM "NEW_YORK_CITIBIKE_1"."NOAA_GSOD"."GSOD2015" w
    JOIN "NEW_YORK_CITIBIKE_1"."NOAA_GSOD"."STATIONS" s ON w."wban" = s."wban"
    JOIN "NEW_YORK_CITIBIKE_1"."GEO_US_BOUNDARIES"."ZIP_CODES" z 
      ON ST_WITHIN(ST_MAKEPOINT(s."lon", s."lat"), z."zip_code_geom")
    WHERE w."year" = '2015' AND w."mo" = '07' AND w."da" = '15' 
      AND w."wban" = '94728' AND w."temp" = (SELECT max_temp FROM highest_temp)
),
qualified_trips AS (
    SELECT 
        start_z."zip_code" as start_zip_code,
        end_z."zip_code" as end_zip_code,
        start_z."area_land_meters" as start_area,
        end_z."area_land_meters" as end_area
    FROM "NEW_YORK_CITIBIKE_1"."NEW_YORK_CITIBIKE"."CITIBIKE_TRIPS" t
    JOIN "NEW_YORK_CITIBIKE_1"."GEO_US_BOUNDARIES"."ZIP_CODES" start_z
      ON ST_WITHIN(ST_MAKEPOINT(t."start_station_longitude", t."start_station_latitude"), start_z."zip_code_geom")
    JOIN "NEW_YORK_CITIBIKE_1"."GEO_US_BOUNDARIES"."ZIP_CODES" end_z
      ON ST_WITHIN(ST_MAKEPOINT(t."end_station_longitude", t."end_station_latitude"), end_z."zip_code_geom")
    WHERE DATEADD('SECOND', t."starttime" / 1000000, '1970-01-01')::DATE = '2015-07-15'
      AND start_z."zip_code" IN (SELECT "zip_code" FROM hot_zip_codes)
      AND end_z."zip_code" IN (SELECT "zip_code" FROM hot_zip_codes)
)
SELECT start_zip_code, end_zip_code
FROM qualified_trips
ORDER BY start_area ASC, end_area DESC
LIMIT 1