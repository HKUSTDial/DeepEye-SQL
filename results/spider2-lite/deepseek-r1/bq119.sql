WITH hurricane_points AS (
  SELECT sid, name, iso_time, latitude, longitude, usa_wind,
    ST_GEOGPOINT(longitude, latitude) AS geo_point
  FROM `bigquery-public-data.noaa_hurricanes.hurricanes`
  WHERE basin = 'NA' AND EXTRACT(YEAR FROM iso_time) = 2020 AND track_type = 'main'
),
with_prev_point AS (
  SELECT *,
    LAG(geo_point) OVER (PARTITION BY sid ORDER BY iso_time) AS prev_geo_point
  FROM hurricane_points
),
with_segment_distance AS (
  SELECT *,
    IFNULL(ST_DISTANCE(geo_point, prev_geo_point), 0) AS segment_distance_m
  FROM with_prev_point
),
hurricane_totals AS (
  SELECT sid,
    SUM(segment_distance_m) AS total_distance_m
  FROM with_segment_distance
  GROUP BY sid
),
ranked_hurricanes AS (
  SELECT sid, total_distance_m,
    RANK() OVER (ORDER BY total_distance_m DESC) AS rank
  FROM hurricane_totals
),
third_hurricane AS (
  SELECT sid, total_distance_m
  FROM ranked_hurricanes
  WHERE rank = 3
),
cumulative_for_third AS (
  SELECT w.sid, w.name, w.iso_time, w.latitude, w.longitude, w.usa_wind,
    SUM(w.segment_distance_m) OVER (PARTITION BY w.sid ORDER BY w.iso_time) AS cumulative_distance_m
  FROM with_segment_distance w
  INNER JOIN third_hurricane t ON w.sid = t.sid
)
SELECT sid, name, iso_time, latitude, longitude,
  cumulative_distance_m / 1000 AS cumulative_distance_km,
  usa_wind
FROM cumulative_for_third
ORDER BY iso_time