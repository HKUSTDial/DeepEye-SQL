WITH station_counts AS (
  SELECT b.neighborhood, COUNT(DISTINCT s.station_id) AS station_count
  FROM `bigquery-public-data.san_francisco_neighborhoods.boundaries` b
  JOIN `bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info` s
    ON ST_WITHIN(s.station_geom, b.neighborhood_geom)
  GROUP BY b.neighborhood
),
incident_counts AS (
  SELECT b.neighborhood, COUNT(DISTINCT i.unique_key) AS incident_count
  FROM `bigquery-public-data.san_francisco_neighborhoods.boundaries` b
  JOIN `bigquery-public-data.san_francisco_sfpd_incidents.sfpd_incidents` i
    ON ST_WITHIN(ST_GEOGPOINT(i.longitude, i.latitude), b.neighborhood_geom)
  WHERE i.latitude IS NOT NULL AND i.longitude IS NOT NULL
  GROUP BY b.neighborhood
)
SELECT s.neighborhood, s.station_count, i.incident_count
FROM station_counts s
JOIN incident_counts i ON s.neighborhood = i.neighborhood
ORDER BY s.neighborhood