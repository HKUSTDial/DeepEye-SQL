WITH denmark_boundary AS (
  SELECT TO_GEOGRAPHY("geometry") as geom
  FROM "GEO_OPENSTREETMAP"."GEO_OPENSTREETMAP"."PLANET_FEATURES"
  WHERE "feature_type" = 'multipolygons'
    AND "all_tags":"wikidata" = 'Q35'
)
SELECT 
  w."all_tags":"highway"::STRING as highway_type,
  SUM(ST_LENGTH(TO_GEOGRAPHY(w."geometry"))) as total_length
FROM "GEO_OPENSTREETMAP"."GEO_OPENSTREETMAP"."PLANET_WAYS" w
JOIN denmark_boundary db ON ST_WITHIN(TO_GEOGRAPHY(w."geometry"), db.geom)
WHERE w."all_tags":"highway" IS NOT NULL
GROUP BY highway_type
ORDER BY total_length DESC
LIMIT 5