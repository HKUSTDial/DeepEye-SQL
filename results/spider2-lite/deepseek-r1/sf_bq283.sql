WITH active_stations AS (
  SELECT "station_id"
  FROM "AUSTIN"."AUSTIN_BIKESHARE"."BIKESHARE_STATIONS"
  WHERE "status" != 'closed'
), station_trips AS (
  SELECT 
    t."start_station_id" AS "station_id",
    COUNT(*) AS "total_trips",
    AVG(t."duration_minutes") AS "avg_duration"
  FROM "AUSTIN"."AUSTIN_BIKESHARE"."BIKESHARE_TRIPS" t
  INNER JOIN active_stations a ON t."start_station_id" = a."station_id"
  GROUP BY t."start_station_id"
), ranked_stations AS (
  SELECT 
    "station_id",
    "total_trips",
    "avg_duration",
    RANK() OVER (ORDER BY "total_trips" DESC) AS "rank",
    SUM("total_trips") OVER () AS "overall_total"
  FROM station_trips
)
SELECT 
  "station_id",
  "total_trips",
  ("total_trips" / "overall_total") * 100 AS "percentage",
  "avg_duration"
FROM ranked_stations
WHERE "rank" <= 15
ORDER BY "rank"