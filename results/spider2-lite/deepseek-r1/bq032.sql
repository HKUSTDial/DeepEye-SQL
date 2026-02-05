WITH storm_points AS (
  SELECT sid, iso_time, latitude, longitude,
    ST_GEOGPOINT(longitude, latitude) AS geo
  FROM `bigquery-public-data.noaa_hurricanes.hurricanes`
  WHERE basin = 'NA' AND season = '2020' AND track_type = 'main'
),
with_prev AS (
  SELECT sid, iso_time, latitude, longitude, geo,
    LAG(geo) OVER (PARTITION BY sid ORDER BY iso_time) AS prev_geo
  FROM storm_points
),
distances AS (
  SELECT sid, iso_time, latitude, longitude,
    CASE 
      WHEN prev_geo IS NULL THEN 0
      ELSE ST_DISTANCE(prev_geo, geo)
    END AS segment_distance
  FROM with_prev
),
total_distances AS (
  SELECT sid, SUM(segment_distance) AS total_distance
  FROM distances
  GROUP BY sid
),
ranked AS (
  SELECT sid, total_distance,
    RANK() OVER (ORDER BY total_distance DESC) AS rank_distance
  FROM total_distances
),
second_sid AS (
  SELECT sid FROM ranked WHERE rank_distance = 2
  ORDER BY sid LIMIT 1
),
final_points AS (
  SELECT sid, iso_time, latitude, longitude,
    ROW_NUMBER() OVER (PARTITION BY sid ORDER BY iso_time DESC) AS rn
  FROM storm_points
  WHERE sid IN (SELECT sid FROM second_sid)
)
SELECT latitude
FROM final_points
WHERE rn = 1