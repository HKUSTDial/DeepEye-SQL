WITH trips_with_region AS (
  SELECT 
    r.region_id,
    r.name AS region_name,
    t.trip_id,
    t.duration_sec,
    t.start_date,
    t.start_station_name,
    t.member_gender
  FROM `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips` t
  JOIN `bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info` s
    ON CAST(s.station_id AS INT64) = t.start_station_id
  JOIN `bigquery-public-data.san_francisco_bikeshare.bikeshare_regions` r
    ON s.region_id = r.region_id
  WHERE t.start_date >= TIMESTAMP('2014-01-01') 
    AND t.start_date < TIMESTAMP('2018-01-01')
)
SELECT 
  region_name,
  trip_id,
  duration_sec,
  start_date,
  start_station_name,
  member_gender
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY region_id ORDER BY start_date DESC) AS rn
  FROM trips_with_region
) ranked
WHERE rn = 1