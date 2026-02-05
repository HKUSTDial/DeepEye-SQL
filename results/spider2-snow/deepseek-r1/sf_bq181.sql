WITH all_stations AS (
  SELECT DISTINCT "usaf", "wban"
  FROM "NOAA_DATA"."NOAA_GSOD"."STATIONS"
  WHERE "usaf" != '999999'
),
valid_days_per_station AS (
  SELECT a."usaf", a."wban", COUNT(DISTINCT g."date") AS valid_days
  FROM all_stations a
  LEFT JOIN "NOAA_DATA"."NOAA_GSOD"."GSOD2022" g
    ON a."usaf" = g."stn" AND a."wban" = g."wban"
    AND EXTRACT(YEAR FROM g."date") = 2022
    AND g."temp" IS NOT NULL AND g."temp" != 9999.9
    AND g."max" IS NOT NULL AND g."max" != 9999.9
    AND g."min" IS NOT NULL AND g."min" != 9999.9
  GROUP BY a."usaf", a."wban"
),
total_days_in_2022 AS (
  SELECT 365 AS total_days
),
stations_meeting_criteria AS (
  SELECT v."usaf", v."wban"
  FROM valid_days_per_station v, total_days_in_2022 t
  WHERE v.valid_days >= 0.9 * t.total_days
),
counts AS (
  SELECT
    (SELECT COUNT(*) FROM stations_meeting_criteria) AS stations_with_valid_data,
    (SELECT COUNT(*) FROM all_stations) AS total_stations
)
SELECT
  stations_with_valid_data * 100.0 / total_stations AS percentage
FROM counts