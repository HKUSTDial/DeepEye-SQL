WITH q191_geom AS (
  SELECT pf.geometry
  FROM "GEO_OPENSTREETMAP"."GEO_OPENSTREETMAP"."PLANET_FEATURES" pf,
  LATERAL FLATTEN(INPUT => pf.all_tags) t
  WHERE pf.feature_type = 'multipolygons'
    AND t.key = 'wikidata'
    AND t.value = 'Q191'
  LIMIT 1
),
multipolygon_tags AS (
  SELECT pf.osm_way_id, pf.geometry, t.key, t.value
  FROM "GEO_OPENSTREETMAP"."GEO_OPENSTREETMAP"."PLANET_FEATURES" pf,
  LATERAL FLATTEN(INPUT => pf.all_tags) t
  WHERE pf.feature_type = 'multipolygons'
    AND ST_WITHIN(pf.geometry, (SELECT geometry FROM q191_geom))
),
candidate_multipolygons AS (
  SELECT osm_way_id, geometry, MAX(CASE WHEN key = 'name' THEN value END) AS name
  FROM multipolygon_tags
  GROUP BY osm_way_id, geometry
  HAVING MAX(CASE WHEN key = 'wikidata' THEN 1 ELSE 0 END) = 0
    AND name IS NOT NULL
),
point_counts AS (
  SELECT cm.osm_way_id, cm.name, COUNT(pn.id) AS point_count
  FROM candidate_multipolygons cm
  JOIN "GEO_OPENSTREETMAP"."GEO_OPENSTREETMAP"."PLANET_NODES" pn
    ON ST_WITHIN(pn.geometry, cm.geometry)
  GROUP BY cm.osm_way_id, cm.name
)
SELECT name, point_count
FROM point_counts
ORDER BY point_count DESC
LIMIT 2