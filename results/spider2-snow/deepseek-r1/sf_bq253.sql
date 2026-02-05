WITH target AS (
  SELECT TO_GEOGRAPHY(geometry) AS target_geom
  FROM "GEO_OPENSTREETMAP"."GEO_OPENSTREETMAP"."PLANET_FEATURES"
  WHERE feature_type = 'multipolygons'
  AND EXISTS (
    SELECT 1 FROM LATERAL FLATTEN(all_tags) t
    WHERE t.value['key'] = 'wikidata' AND t.value['value'] = 'Q1095'
  )
  LIMIT 1
), candidate_relations AS (
  SELECT r.id, TO_GEOGRAPHY(r.geometry) AS relation_geom,
    (SELECT t.value['value'] FROM LATERAL FLATTEN(r.all_tags) t WHERE t.value['key'] = 'name' LIMIT 1) AS relation_name
  FROM "GEO_OPENSTREETMAP"."GEO_OPENSTREETMAP"."PLANET_RELATIONS" r
  WHERE EXISTS (SELECT 1 FROM LATERAL FLATTEN(r.all_tags) t WHERE t.value['key'] = 'name')
  AND NOT EXISTS (SELECT 1 FROM LATERAL FLATTEN(r.all_tags) t WHERE t.value['key'] = 'wikidata')
), features_in_target AS (
  SELECT TO_GEOGRAPHY(f.geometry) AS feature_geom, f.all_tags AS feature_tags
  FROM "GEO_OPENSTREETMAP"."GEO_OPENSTREETMAP"."PLANET_FEATURES" f, target
  WHERE ST_WITHIN(TO_GEOGRAPHY(f.geometry), target.target_geom)
), relation_features AS (
  SELECT cr.id, cr.relation_name, ft.feature_geom, ft.feature_tags
  FROM candidate_relations cr
  JOIN features_in_target ft ON ST_COVERS(cr.relation_geom, ft.feature_geom)
), relation_counts AS (
  SELECT rf.id, rf.relation_name,
    COUNT(*) AS feature_count,
    SUM(CASE WHEN EXISTS (SELECT 1 FROM LATERAL FLATTEN(rf.feature_tags) t WHERE t.value['key'] = 'wikidata') THEN 1 ELSE 0 END) AS wikidata_features_count
  FROM relation_features rf
  GROUP BY rf.id, rf.relation_name
  HAVING wikidata_features_count >= 1
)
SELECT relation_name
FROM relation_counts
ORDER BY feature_count DESC
LIMIT 1