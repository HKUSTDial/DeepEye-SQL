WITH denmark AS (
  SELECT TO_GEOGRAPHY("geometry") AS denmark_geom
  FROM "GEO_OPENSTREETMAP"."GEO_OPENSTREETMAP"."PLANET_FEATURES"
  WHERE "feature_type" = 'multipolygons'
    AND EXISTS (
      SELECT 1
      FROM LATERAL FLATTEN(INPUT => "all_tags") AS tag
      WHERE tag:key::STRING = 'wikidata' AND tag:value::STRING = 'Q35'
    )
),
bus_stop_candidates AS (
  SELECT 
    p."osm_id",
    TO_GEOGRAPHY(p."geometry") AS point_geom,
    MAX(CASE WHEN tag:key::STRING = 'highway' AND tag:value::STRING = 'bus_stop' THEN 1 
             WHEN tag:key::STRING = 'public_transport' AND tag:value::STRING = 'stop_position' THEN 1 
             ELSE 0 END) AS is_bus_stop,
    MAX(CASE WHEN tag:key::STRING = 'network' THEN tag:value::STRING END) AS network
  FROM "GEO_OPENSTREETMAP"."GEO_OPENSTREETMAP"."PLANET_FEATURES_POINTS" p,
  LATERAL FLATTEN(INPUT => p."all_tags") AS tag
  GROUP BY p."osm_id", p."geometry"
)
SELECT COALESCE(MAX(stop_count), 0) AS max_bus_stop_count
FROM (
  SELECT b.network, COUNT(*) AS stop_count
  FROM bus_stop_candidates b, denmark d
  WHERE b.is_bus_stop = 1
    AND b.network IS NOT NULL
    AND ST_WITHIN(b.point_geom, d.denmark_geom)
  GROUP BY b.network
)