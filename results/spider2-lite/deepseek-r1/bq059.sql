SELECT ROUND(MAX(ST_DISTANCE(t.start_station_geom, t.end_station_geom) / t.duration_sec), 1) AS highest_avg_speed
FROM `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips` AS t
JOIN `bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info` AS info ON CAST(t.start_station_id AS STRING) = info.station_id
JOIN `bigquery-public-data.san_francisco_bikeshare.bikeshare_regions` AS r ON info.region_id = r.region_id
WHERE r.name = 'Berkeley' AND ST_DISTANCE(t.start_station_geom, t.end_station_geom) > 1000 AND t.duration_sec > 0