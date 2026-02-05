WITH q191_geom AS (
  SELECT TO_GEOGRAPHY(pf."geometry") AS geom
  FROM "GEO_OPENSTREETMAP"."GEO_OPENSTREETMAP"."PLANET_FEATURES" pf,
  LATERAL FLATTEN(INPUT => pf."all_tags") t
  WHERE pf."feature_type" = 'multipolygons'
    AND t.value:key::STRING = 'wikidata'
    AND t.value:value::STRING = 'Q191'
  LIMIT 1
),
candidate_multipolygons AS (
  SELECT 
    pf."osm_id",
    pf."osm_way_id",
    TO_GEOGRAPHY(pf."geometry") AS geom,
    pf."all_tags"
  FROM "GEO_OPENSTREETMAP"."GEO_OPENSTREETMAP"."PLANET_FEATURES" pf
  LEFT JOIN LATERAL FLATTEN(INPUT => pf."all_tags") t_wikidata 
    ON t_wikidata.value:key::STRING = 'wikidata'
  WHERE pf."feature_type" = 'multipolygons'
    AND t_wikidata.value:key IS NULL
    AND ST_DWITHIN(TO_GEOGRAPHY(pf."geometry"), (SELECT geom FROM q191_geom), 0)
),
point_counts AS (
  SELECT 
    cand."osm_id",
    cand."osm_way_id",
    cand."geom",
    cand."all_tags",
    COUNT(pn."id") AS point_count
  FROM candidate_multipolygons cand
  INNER JOIN "GEO_OPENSTREETMAP"."GEO_OPENSTREETMAP"."PLANET_NODES" pn
    ON ST_WITHIN(TO_GEOGRAPHY(pn."geometry"), cand."geom")
  GROUP BY cand."osm_id", cand."osm_way_id", cand."geom", cand."all_tags"
)
SELECT 
  name,
  point_count
FROM (
  SELECT 
    (SELECT t.value:value::STRING 
     FROM LATERAL FLATTEN(INPUT => pc."all_tags") t 
     WHERE t.value:key::STRING = 'name' 
     LIMIT 1) AS name,
    pc.point_count,
    ROW_NUMBER() OVER (ORDER BY pc.point_count DESC) AS rn
  FROM point_counts pc
)
WHERE rn <= 2