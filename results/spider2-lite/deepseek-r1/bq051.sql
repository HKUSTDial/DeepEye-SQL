WITH nearest_station AS (
  SELECT 
    s.id,
    ST_DISTANCE(
      ST_GEOGPOINT(s.longitude, s.latitude),
      ST_GEOGPOINT(-74.0060, 40.7128)
    ) AS distance_meters
  FROM `bigquery-public-data.ghcn_d.ghcnd_stations` s
  WHERE EXISTS (
    SELECT 1 FROM `bigquery-public-data.ghcn_d.ghcnd_2016` g
    WHERE g.id = s.id
      AND g.element = 'PRCP'
      AND g.qflag IS NULL
  )
    AND ST_DISTANCE(
      ST_GEOGPOINT(s.longitude, s.latitude),
      ST_GEOGPOINT(-74.0060, 40.7128)
    ) <= 50000
  ORDER BY distance_meters
  LIMIT 1
),
precipitation_data AS (
  SELECT 
    g.date,
    CASE WHEN g.value / 10.0 > 5 THEN 'rainy' ELSE 'non-rainy' END AS category
  FROM `bigquery-public-data.ghcn_d.ghcnd_2016` g
  JOIN nearest_station ns ON g.id = ns.id
  WHERE g.element = 'PRCP'
    AND g.qflag IS NULL
    AND EXTRACT(YEAR FROM g.date) = 2016
),
citibike_daily AS (
  SELECT 
    DATE(starttime) AS date,
    COUNT(*) AS trip_count
  FROM `bigquery-public-data.new_york.citibike_trips`
  WHERE EXTRACT(YEAR FROM starttime) = 2016
  GROUP BY date
)
SELECT 
  p.category,
  AVG(c.trip_count) AS avg_daily_trips
FROM precipitation_data p
JOIN citibike_daily c ON p.date = c.date
GROUP BY p.category