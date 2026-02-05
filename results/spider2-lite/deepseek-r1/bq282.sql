WITH active_stations AS (
  SELECT station_id, council_district
  FROM `bigquery-public-data.austin_bikeshare.bikeshare_stations`
  WHERE status != 'closed' AND council_district IS NOT NULL
)
SELECT council_district
FROM (
  SELECT start_stations.council_district AS council_district, COUNT(*) AS trip_count
  FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips` AS trips
  INNER JOIN active_stations AS start_stations ON trips.start_station_id = start_stations.station_id
  INNER JOIN active_stations AS end_stations ON SAFE_CAST(trips.end_station_id AS INT64) = end_stations.station_id
  WHERE start_stations.council_district = end_stations.council_district
    AND trips.start_station_id != SAFE_CAST(trips.end_station_id AS INT64)
  GROUP BY start_stations.council_district
)
ORDER BY trip_count DESC
LIMIT 1