WITH all_data AS (
    SELECT "air_temperature", "wetbulb_temperature", "dewpoint_temperature", "sea_surface_temp", "year", "month"
    FROM "NOAA_DATA"."NOAA_ICOADS"."ICOADS_CORE_2010"
    WHERE "year" BETWEEN 2010 AND 2014
    UNION ALL
    SELECT "air_temperature", "wetbulb_temperature", "dewpoint_temperature", "sea_surface_temp", "year", "month"
    FROM "NOAA_DATA"."NOAA_ICOADS"."ICOADS_CORE_2011"
    WHERE "year" BETWEEN 2010 AND 2014
    UNION ALL
    SELECT "air_temperature", "wetbulb_temperature", "dewpoint_temperature", "sea_surface_temp", "year", "month"
    FROM "NOAA_DATA"."NOAA_ICOADS"."ICOADS_CORE_2012"
    WHERE "year" BETWEEN 2010 AND 2014
    UNION ALL
    SELECT "air_temperature", "wetbulb_temperature", "dewpoint_temperature", "sea_surface_temp", "year", "month"
    FROM "NOAA_DATA"."NOAA_ICOADS"."ICOADS_CORE_2013"
    WHERE "year" BETWEEN 2010 AND 2014
    UNION ALL
    SELECT "air_temperature", "wetbulb_temperature", "dewpoint_temperature", "sea_surface_temp", "year", "month"
    FROM "NOAA_DATA"."NOAA_ICOADS"."ICOADS_CORE_2014"
    WHERE "year" BETWEEN 2010 AND 2014
),
monthly_avgs AS (
    SELECT 
        "year", 
        "month",
        AVG("air_temperature") AS avg_air,
        AVG("wetbulb_temperature") AS avg_wetbulb,
        AVG("dewpoint_temperature") AS avg_dewpoint,
        AVG("sea_surface_temp" / 10.0) AS avg_sst
    FROM all_data
    GROUP BY "year", "month"
    HAVING avg_air IS NOT NULL 
       AND avg_wetbulb IS NOT NULL 
       AND avg_dewpoint IS NOT NULL 
       AND avg_sst IS NOT NULL
),
monthly_sums AS (
    SELECT 
        "year", 
        "month",
        ABS(avg_air - avg_wetbulb) + ABS(avg_air - avg_dewpoint) + ABS(avg_air - avg_sst) +
        ABS(avg_wetbulb - avg_dewpoint) + ABS(avg_wetbulb - avg_sst) + ABS(avg_dewpoint - avg_sst) AS sum_abs_diff
    FROM monthly_avgs
)
SELECT "year", "month", sum_abs_diff
FROM monthly_sums
ORDER BY sum_abs_diff ASC
LIMIT 3