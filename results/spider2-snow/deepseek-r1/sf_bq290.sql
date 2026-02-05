WITH daily_country_avgs AS (
  SELECT 
    g."date",
    s."country",
    AVG(g."max") AS avg_max,
    AVG(g."min") AS avg_min,
    AVG(g."temp") AS avg_temp
  FROM "NOAA_DATA"."NOAA_GSOD"."GSOD2023" AS g
  JOIN "NOAA_DATA"."NOAA_GSOD"."STATIONS" AS s
    ON g."stn" = s."usaf" AND g."wban" = s."wban"
  WHERE g."date" >= '2023-10-01' AND g."date" <= '2023-10-31'
    AND g."max" != 9999.9
    AND g."min" != 9999.9
    AND g."temp" != 9999.9
    AND s."country" IN ('US', 'GB')
  GROUP BY g."date", s."country"
),
pivoted AS (
  SELECT
    "date",
    MAX(CASE WHEN "country" = 'US' THEN avg_max END) AS us_avg_max,
    MAX(CASE WHEN "country" = 'GB' THEN avg_max END) AS uk_avg_max,
    MAX(CASE WHEN "country" = 'US' THEN avg_min END) AS us_avg_min,
    MAX(CASE WHEN "country" = 'GB' THEN avg_min END) AS uk_avg_min,
    MAX(CASE WHEN "country" = 'US' THEN avg_temp END) AS us_avg_temp,
    MAX(CASE WHEN "country" = 'GB' THEN avg_temp END) AS uk_avg_temp
  FROM daily_country_avgs
  GROUP BY "date"
)
SELECT
  "date",
  us_avg_max - uk_avg_max AS diff_max_temp,
  us_avg_min - uk_avg_min AS diff_min_temp,
  us_avg_temp - uk_avg_temp AS diff_avg_temp
FROM pivoted
ORDER BY "date"