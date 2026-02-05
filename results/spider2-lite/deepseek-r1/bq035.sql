SELECT 
  t.bike_number,
  SUM(ST_DISTANCE(ST_GEOGPOINT(s.longitude, s.latitude), 
                  ST_GEOGPOINT(e.longitude, e.latitude))) AS total_distance_meters
FROM `bigquery-public-data.san_francisco.bikeshare_trips` AS t
JOIN `bigquery-public-data.san_francisco.bikeshare_stations` AS s 
  ON t.start_station_id = s.station_id
JOIN `bigquery-public-data.san_francisco.bikeshare_stations` AS e 
  ON t.end_station_id = e.station_id
GROUP BY t.bike_number
ORDER BY t.bike_number