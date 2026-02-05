WITH eligible_stations AS (
  SELECT
    "usaf",
    "wban",
    TO_DATE("begin", 'YYYYMMDD') AS begin_date,
    TO_DATE("end", 'YYYYMMDD') AS end_date
  FROM "NOAA_DATA"."NOAA_GSOD"."STATIONS"
  WHERE TO_DATE("begin", 'YYYYMMDD') <= '2000-01-01'
    AND TO_DATE("end", 'YYYYMMDD') >= '2019-06-30'
),
max_possible_days_per_station AS (
  SELECT
    "usaf",
    "wban",
    DATEDIFF('day', DATE '2019-01-01', LEAST(end_date, DATE '2019-12-31')) + 1 AS max_days
  FROM eligible_stations
),
actual_valid_days_per_station AS (
  SELECT
    e."usaf",
    e."wban",
    COUNT(DISTINCT DATE_FROM_PARTS(CAST(g."year" AS INTEGER), CAST(g."mo" AS INTEGER), CAST(g."da" AS INTEGER))) AS actual_days
  FROM eligible_stations e
  INNER JOIN "NOAA_DATA"."NOAA_GSOD"."GSOD2019" g
    ON e."usaf" = g."stn"
    AND e."wban" = g."wban"
  WHERE g."temp" != 9999.9
    AND g."max" != 9999.9
    AND g."min" != 9999.9
    AND DATE_FROM_PARTS(CAST(g."year" AS INTEGER), CAST(g."mo" AS INTEGER), CAST(g."da" AS INTEGER))
        BETWEEN DATE '2019-01-01' AND LEAST(e.end_date, DATE '2019-12-31')
  GROUP BY e."usaf", e."wban"
),
stations_with_ratio AS (
  SELECT
    e."usaf",
    e."wban",
    a.actual_days,
    m.max_days,
    a.actual_days / m.max_days AS ratio
  FROM eligible_stations e
  LEFT JOIN actual_valid_days_per_station a ON e."usaf" = a."usaf" AND e."wban" = a."wban"
  INNER JOIN max_possible_days_per_station m ON e."usaf" = m."usaf" AND e."wban" = m."wban"
  WHERE a.actual_days / m.max_days >= 0.9
)
SELECT COUNT(*) AS station_count
FROM stations_with_ratio