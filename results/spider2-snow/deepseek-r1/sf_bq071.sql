SELECT
    "z"."city",
    "z"."zip_code",
    "z"."state_name" AS "state",
    COUNT(DISTINCT "h"."sid") AS "count of hurricanes",
    LISTAGG(DISTINCT "h"."name", ', ') WITHIN GROUP (ORDER BY "h"."name") AS "list of hurricanes"
FROM "NOAA_DATA_PLUS"."NOAA_HURRICANES"."HURRICANES" AS "h"
JOIN "NOAA_DATA_PLUS"."GEO_US_BOUNDARIES"."ZIP_CODES" AS "z"
    ON ST_WITHIN(ST_MAKEPOINT("h"."longitude", "h"."latitude"), TO_GEOGRAPHY("z"."zip_code_geom"))
WHERE "h"."name" IS NOT NULL AND "h"."name" != ''
GROUP BY "z"."city", "z"."zip_code", "z"."state_name"
ORDER BY "count of hurricanes" DESC