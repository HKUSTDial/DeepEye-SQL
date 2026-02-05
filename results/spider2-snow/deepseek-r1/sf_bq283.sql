WITH active_stations AS (
  SELECT "station_id"
  FROM "AUSTIN"."AUSTIN_BIKESHARE"."BIKESHARE_STATIONS"
  WHERE "status" != 'closed'
),
trips_active AS (
  SELECT t."trip_id", t."duration_minutes", t."start_station_id"
  FROM "AUSTIN"."AUSTIN_BIKESHARE"."BIKESHARE_TRIPS" t
  INNER JOIN active_stations a ON t."start_station_id" = a."station_id"
),
per_station AS (
  SELECT 
    "start_station_id" AS station_id,
    COUNT(*) AS trip_count,
    AVG("duration_minutes") AS avg_duration
  FROM trips_active
  GROUP BY "start_station_id"
),
overall_total AS (
  SELECT SUM(trip_count) AS total_trips
  FROM per_station
),
ranked_stations AS (
  SELECT 
    station_id,
    trip_count,
    avg_duration,
    DENSE_RANK() OVER (ORDER BY trip_count DESC) AS station_rank
  FROM per_station
)
SELECT 
  rs.station_id,
  rs.trip_count,
  (rs.trip_count * 100.0 / ot.total_trips) AS percentage,
  rs.avg_duration
FROM ranked_stations rs
CROSS JOIN overall_total ot
WHERE rs.station_rank <= 15
ORDER BY rs.station_rank