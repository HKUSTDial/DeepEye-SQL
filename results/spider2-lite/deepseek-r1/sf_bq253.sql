WITH multipolygon AS (
    SELECT TO_GEOGRAPHY(geometry) AS geom
    FROM "GEO_OPENSTREETMAP"."GEO_OPENSTREETMAP"."PLANET_FEATURES"
    WHERE feature_type = 'multipolygons'
      AND all_tags['wikidata']::string = 'Q1095'
),
features_in_area AS (
    SELECT 
        pf.osm_id,
        TO_GEOGRAPHY(pf.geometry) AS geom,
        CASE WHEN pf.all_tags['wikidata'] IS NOT NULL THEN 1 ELSE 0 END AS has_wikidata
    FROM "GEO_OPENSTREETMAP"."GEO_OPENSTREETMAP"."PLANET_FEATURES" pf
    CROSS JOIN multipolygon mp
    WHERE ST_WITHIN(TO_GEOGRAPHY(pf.geometry), mp.geom)
      AND NOT (pf.feature_type = 'multipolygons' AND pf.all_tags['wikidata']::string = 'Q1095')
),
relation_counts AS (
    SELECT 
        r.id AS relation_id,
        r.all_tags['name']::string AS relation_name,
        COUNT(f.osm_id) AS num_features,
        MAX(f.has_wikidata) AS has_any_wikidata
    FROM "GEO_OPENSTREETMAP"."GEO_OPENSTREETMAP"."PLANET_RELATIONS" r
    INNER JOIN features_in_area f
        ON ST_COVERS(TO_GEOGRAPHY(r.geometry), f.geom)
    WHERE r.all_tags['name'] IS NOT NULL
      AND r.all_tags['wikidata'] IS NULL
    GROUP BY r.id, r.all_tags['name']
    HAVING has_any_wikidata = 1
)
SELECT relation_name
FROM relation_counts
ORDER BY num_features DESC
LIMIT 1