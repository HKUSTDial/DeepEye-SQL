WITH area_polygon AS (
  SELECT ST_MAKEPOLYGON(TO_GEOGRAPHY('LINESTRING(31.1798246 18.4519921, 54.3798246 18.4519921, 54.3798246 33.6519921, 31.1798246 33.6519921, 31.1798246 18.4519921)')) AS polygon
),
historical_nodes_in_area AS (
  SELECT "id", "username", "geometry", "all_tags"
  FROM "GEO_OPENSTREETMAP"."GEO_OPENSTREETMAP"."HISTORY_NODES"
),
historical_nodes_with_tags AS (
  SELECT DISTINCT hn."id", hn."username"
  FROM historical_nodes_in_area hn
  CROSS JOIN area_polygon ap
  INNER JOIN LATERAL FLATTEN(input => hn."all_tags") t
  WHERE t."key" = 'amenity' AND t."value" IN ('hospital', 'clinic', 'doctors')
    AND ST_INTERSECTS(TO_GEOGRAPHY(hn."geometry"), ap."polygon")
),
nodes_not_in_planet AS (
  SELECT hnt."id", hnt."username"
  FROM historical_nodes_with_tags hnt
  LEFT JOIN "GEO_OPENSTREETMAP"."GEO_OPENSTREETMAP"."PLANET_NODES" pn ON hnt."id" = pn."id"
  WHERE pn."id" IS NULL
)
SELECT "username", COUNT("id") AS node_count
FROM nodes_not_in_planet
GROUP BY "username"
ORDER BY node_count DESC
LIMIT 3