SELECT 
    "ZIP_CODES"."city",
    "ZIP_CODES"."zip_code",
    "ZIP_CODES"."state_name" AS "state",
    COUNT(DISTINCT "HURRICANES"."name") AS "count of hurricanes",
    LISTAGG(DISTINCT "HURRICANES"."name", ', ') WITHIN GROUP (ORDER BY "HURRICANES"."name") AS "list of hurricanes"
FROM 
    "NOAA_DATA_PLUS"."NOAA_HURRICANES"."HURRICANES"
JOIN 
    "NOAA_DATA_PLUS"."GEO_US_BOUNDARIES"."ZIP_CODES"
    ON ST_WITHIN(
        ST_POINT("HURRICANES"."longitude", "HURRICANES"."latitude"),
        TO_GEOGRAPHY("ZIP_CODES"."zip_code_geom")
    )
WHERE 
    "HURRICANES"."name" IS NOT NULL 
    AND TRIM("HURRICANES"."name") != ''
GROUP BY 
    "ZIP_CODES"."city",
    "ZIP_CODES"."zip_code",
    "ZIP_CODES"."state_name"
ORDER BY 
    "count of hurricanes" DESC