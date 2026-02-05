WITH eligible_storms AS (
  SELECT 
    `sid` AS storm_id,
    ST_CONVEXHULL(ST_UNION_AGG(ST_GEOGPOINT(`longitude`, `latitude`))) AS storm_area
  FROM `bigquery-public-data.noaa_hurricanes.hurricanes`
  WHERE `basin` = 'NA'
    AND `name` != 'NOT_NAMED'
    AND `usa_wind` >= 35
    AND `usa_wind` IS NOT NULL
  GROUP BY `sid`
  HAVING COUNT(*) >= 1
),
us_ports AS (
  SELECT 
    p.`index_number`,
    p.`port_name`,
    ANY_VALUE(p.`port_geom`) AS port_geom
  FROM `bigquery-public-data.geo_international_ports.world_port_index` p
  JOIN `bigquery-public-data.geo_us_boundaries.states` s
    ON ST_WITHIN(p.`port_geom`, s.`state_geom`)
  WHERE p.`region_number` = '6585'
  GROUP BY p.`index_number`, p.`port_name`
),
port_storm_counts AS (
  SELECT 
    up.`index_number`,
    up.`port_name`,
    COUNT(DISTINCT es.storm_id) AS storm_count
  FROM us_ports up
  CROSS JOIN eligible_storms es
  WHERE ST_WITHIN(up.`port_geom`, es.storm_area)
  GROUP BY up.`index_number`, up.`port_name`
)
SELECT 
  `port_name`,
  `index_number`,
  storm_count
FROM port_storm_counts
ORDER BY storm_count DESC
LIMIT 1