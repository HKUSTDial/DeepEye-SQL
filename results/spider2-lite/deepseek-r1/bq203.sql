WITH total_per_borough AS (
  SELECT borough_name, COUNT(DISTINCT station_id) AS total_stations
  FROM `bigquery-public-data.new_york_subway.stations`
  GROUP BY borough_name
),
ada_entry_stations AS (
  SELECT DISTINCT s.station_id, s.borough_name
  FROM `bigquery-public-data.new_york_subway.stations` s
  INNER JOIN `bigquery-public-data.new_york_subway.station_entrances` e
    ON s.station_name = e.station_name
  WHERE e.entry = TRUE AND e.ada_compliant = TRUE
),
ada_per_borough AS (
  SELECT borough_name, COUNT(DISTINCT station_id) AS ada_stations_count
  FROM ada_entry_stations
  GROUP BY borough_name
)
SELECT 
  t.borough_name,
  t.total_stations,
  COALESCE(a.ada_stations_count, 0) AS stations_with_ada_entry,
  ROUND(COALESCE(a.ada_stations_count, 0) * 100.0 / t.total_stations, 2) AS percentage
FROM total_per_borough t
LEFT JOIN ada_per_borough a ON t.borough_name = a.borough_name
ORDER BY percentage DESC