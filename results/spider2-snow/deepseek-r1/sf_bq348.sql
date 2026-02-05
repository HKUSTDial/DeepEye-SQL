WITH rect AS (
  SELECT ST_MAKEPOLYGON(TO_GEOGRAPHY('LINESTRING(31.1798246 18.4519921, 54.3798246 18.4519921, 54.3798246 33.6519921, 31.1798246 33.6519921, 31.1798246 18.4519921)')) AS polygon
),
tagged_nodes AS (
  SELECT DISTINCT hn."id", hn."username", hn."latitude", hn."longitude"
  FROM "GEO_OPENSTREETMAP"."GEO_OPENSTREETMAP"."HISTORY_NODES" hn,
  LATERAL FLATTEN(input => hn."all_tags") t
  WHERE t."KEY" = 'amenity' AND t."VALUE" IN ('hospital', 'clinic', 'doctors')
),
tagged_in_rect AS (
  SELECT tn."id", tn."username"
  FROM tagged_nodes tn
  CROSS JOIN rect
  WHERE ST_INTERSECTS(ST_MAKEPOINT(tn."longitude", tn."latitude"), rect.polygon)
),
missing_nodes AS (
  SELECT tnr."id", tnr."username"
  FROM tagged_in_rect tnr
  LEFT JOIN "GEO_OPENSTREETMAP"."GEO_OPENSTREETMAP"."PLANET_NODES" pn ON tnr."id" = pn."id"
  WHERE pn."id" IS NULL
)
SELECT "username", COUNT(*) AS node_count
FROM missing_nodes
GROUP BY "username"
ORDER BY node_count DESC
LIMIT 3