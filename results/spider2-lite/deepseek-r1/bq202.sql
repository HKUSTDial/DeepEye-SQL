WITH top_station AS (
  SELECT start_station_id
  FROM `bigquery-public-data.new_york_citibike.citibike_trips`
  WHERE EXTRACT(YEAR FROM starttime) = 2018
  GROUP BY start_station_id
  ORDER BY COUNT(*) DESC
  LIMIT 1
)
SELECT 
  EXTRACT(DAYOFWEEK FROM t.starttime) AS day_of_week,
  EXTRACT(HOUR FROM t.starttime) AS hour_of_day
FROM `bigquery-public-data.new_york_citibike.citibike_trips` t
JOIN top_station ts ON t.start_station_id = ts.start_station_id
WHERE EXTRACT(YEAR FROM t.starttime) = 2018
GROUP BY day_of_week, hour_of_day
ORDER BY COUNT(*) DESC
LIMIT 1