WITH candidate_stations AS (
  SELECT 
    "id",
    "latitude",
    "longitude",
    ( 6371 * ACOS( COS(RADIANS(40.7128)) * COS(RADIANS("latitude")) * COS(RADIANS("longitude") - RADIANS(-74.0060)) + SIN(RADIANS(40.7128)) * SIN(RADIANS("latitude")) ) ) AS distance_km
  FROM "NEW_YORK_GHCN"."GHCN_D"."GHCND_STATIONS"
),
stations_with_prcp AS (
  SELECT DISTINCT "id"
  FROM "NEW_YORK_GHCN"."GHCN_D"."GHCND_2016"
  WHERE "element" = 'PRCP'
    AND ("qflag" IS NULL OR "qflag" = 'NULL')
),
nearest_station AS (
  SELECT cs."id"
  FROM candidate_stations cs
  INNER JOIN stations_with_prcp sp ON cs."id" = sp."id"
  WHERE cs.distance_km <= 50
  ORDER BY cs.distance_km ASC
  LIMIT 1
),
daily_precipitation AS (
  SELECT 
    "date",
    "value"/10.0 AS precipitation_mm,
    CASE WHEN "value"/10.0 > 5 THEN 'rainy' ELSE 'non-rainy' END AS rain_flag
  FROM "NEW_YORK_GHCN"."GHCN_D"."GHCND_2016"
  WHERE "id" = (SELECT "id" FROM nearest_station)
    AND "element" = 'PRCP'
    AND ("qflag" IS NULL OR "qflag" = 'NULL')
    AND "date" BETWEEN '2016-01-01' AND '2016-12-31'
),
daily_trips AS (
  SELECT 
    DATE(CONVERT_TIMEZONE('UTC', 'America/New_York', TO_TIMESTAMP("starttime" / 1000000))) AS trip_date,
    COUNT(*) AS trip_count
  FROM "NEW_YORK_GHCN"."NEW_YORK"."CITIBIKE_TRIPS"
  WHERE CONVERT_TIMEZONE('UTC', 'America/New_York', TO_TIMESTAMP("starttime" / 1000000)) >= '2016-01-01'::TIMESTAMP_NTZ
    AND CONVERT_TIMEZONE('UTC', 'America/New_York', TO_TIMESTAMP("starttime" / 1000000)) < '2017-01-01'::TIMESTAMP_NTZ
  GROUP BY trip_date
)
SELECT 
  dp.rain_flag,
  AVG(dt.trip_count) AS avg_daily_trips
FROM daily_precipitation dp
INNER JOIN daily_trips dt ON dp."date" = dt.trip_date
GROUP BY dp.rain_flag