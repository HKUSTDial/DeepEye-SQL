WITH epa_1990 AS (
    SELECT 
        ROUND("latitude", 2) AS lat_r,
        ROUND("longitude", 2) AS lon_r,
        AVG("arithmetic_mean") AS epa_avg
    FROM "OPENAQ"."EPA_HISTORICAL_AIR_QUALITY"."AIR_QUALITY_ANNUAL_SUMMARY"
    WHERE "parameter_name" LIKE '%PM2.5%'
      AND "units_of_measure" = 'Micrograms/cubic meter (LC)'
      AND "year" = 1990
    GROUP BY ROUND("latitude", 2), ROUND("longitude", 2)
),
openaq_2020 AS (
    SELECT 
        ROUND("latitude", 2) AS lat_r,
        ROUND("longitude", 2) AS lon_r,
        "city",
        AVG("value") AS openaq_avg
    FROM "OPENAQ"."OPENAQ"."GLOBAL_AIR_QUALITY"
    WHERE "pollutant" = 'pm25'
      AND EXTRACT(YEAR FROM TO_TIMESTAMP("timestamp" / 1000000)) = 2020
    GROUP BY ROUND("latitude", 2), ROUND("longitude", 2), "city"
)
SELECT 
    openaq_2020."city",
    (epa_1990.epa_avg - openaq_2020.openaq_avg) AS difference
FROM epa_1990
INNER JOIN openaq_2020 
    ON epa_1990.lat_r = openaq_2020.lat_r 
    AND epa_1990.lon_r = openaq_2020.lon_r
ORDER BY difference DESC
LIMIT 3