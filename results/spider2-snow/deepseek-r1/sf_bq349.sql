WITH admin_boundaries AS (
  SELECT DISTINCT
    pf."osm_way_id" AS boundary_id,
    ST_GEOGRAPHYFROMWKB(pf."geometry") AS boundary_geo
  FROM
    "GEO_OPENSTREETMAP"."GEO_OPENSTREETMAP"."PLANET_FEATURES" pf,
    TABLE(FLATTEN(input => pf."all_tags")) t
  WHERE
    pf."feature_type" = 'multipolygons'
    AND t.value:key::STRING = 'boundary'
    AND t.value:value::STRING = 'administrative'
),
amenity_nodes AS (
  SELECT DISTINCT
    n."id",
    ST_MAKEPOINT(n."longitude", n."latitude") AS point_geo
  FROM
    "GEO_OPENSTREETMAP"."GEO_OPENSTREETMAP"."PLANET_NODES" n,
    TABLE(FLATTEN(input => n."all_tags")) nt
  WHERE
    nt.value:key::STRING = 'amenity'
),
boundary_counts AS (
  SELECT
    ab.boundary_id,
    COUNT(DISTINCT an."id") AS amenity_count
  FROM
    admin_boundaries ab
    LEFT JOIN amenity_nodes an
      ON ST_WITHIN(an.point_geo, ab.boundary_geo)
  GROUP BY ab.boundary_id
),
median_val AS (
  SELECT MEDIAN(amenity_count) AS median_count FROM boundary_counts
)
SELECT
  bc.boundary_id
FROM
  boundary_counts bc,
  median_val mv
ORDER BY
  ABS(bc.amenity_count - mv.median_count),
  bc.boundary_id
LIMIT 1