WITH wa_stations AS (
  SELECT "usaf", "wban", "name"
  FROM "NOAA_DATA"."NOAA_GSOD"."STATIONS"
  WHERE "state" = 'WA'
), rainy_days_2023 AS (
  SELECT "stn", "wban", COUNT(*) AS rainy_count_2023
  FROM "NOAA_DATA"."NOAA_GSOD"."GSOD2023"
  WHERE "prcp" > 0 AND "prcp" != 99.99
  GROUP BY "stn", "wban"
), rainy_days_2022 AS (
  SELECT "stn", "wban", COUNT(*) AS rainy_count_2022
  FROM "NOAA_DATA"."NOAA_GSOD"."GSOD2022"
  WHERE "prcp" > 0 AND "prcp" != 99.99
  GROUP BY "stn", "wban"
)
SELECT s."name", s."usaf", s."wban"
FROM wa_stations s
INNER JOIN rainy_days_2023 r23 ON s."usaf" = r23."stn" AND s."wban" = r23."wban"
INNER JOIN rainy_days_2022 r22 ON s."usaf" = r22."stn" AND s."wban" = r22."wban"
WHERE r23.rainy_count_2023 > 150 AND r23.rainy_count_2023 < r22.rainy_count_2022