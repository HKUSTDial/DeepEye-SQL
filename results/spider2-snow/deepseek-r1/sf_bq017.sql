WITH denmark AS (
  SELECT TO_GEOGRAPHY(pf."geometry") AS geom
  FROM "GEO_OPENSTREETMAP"."GEO_OPENSTREETMAP"."PLANET_FEATURES" pf,
  LATERAL FLATTEN(INPUT => pf."all_tags") tags
  WHERE pf."feature_type" = 'multipolygons'
    AND tags.value:"k" = 'wikidata'
    AND tags.value:"v" = 'Q35'
),
highway_tags AS (
  SELECT w."id", w."geometry", tags.value:"v" AS highway_type
  FROM "GEO_OPENSTREETMAP"."GEO_OPENSTREETMAP"."PLANET_WAYS" w,
  LATERAL FLATTEN(INPUT => w."all_tags") tags
  WHERE tags.value:"k" = 'highway'
)
SELECT h.highway_type,
  SUM(ST_LENGTH(ST_INTERSECTION(TO_GEOGRAPHY(h."geometry"), d.geom))) AS total_length
FROM highway_tags h
CROSS JOIN denmark d
WHERE ST_INTERSECTS(TO_GEOGRAPHY(h."geometry"), d.geom)
GROUP BY h.highway_type
ORDER BY total_length DESC
LIMIT 5