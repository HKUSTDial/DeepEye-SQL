WITH singapore_grid_all AS (
  SELECT
    "geo_id",
    "longitude_centroid",
    "latitude_centroid",
    "population",
    "last_updated",
    ST_MAKEPOINT("longitude_centroid", "latitude_centroid") AS point_geog
  FROM "GEO_OPENSTREETMAP_WORLDPOP"."WORLDPOP"."POPULATION_GRID_1KM"
  WHERE "country_name" = 'Singapore'
    AND "last_updated" < '2023-01-01'
),
max_date AS (
  SELECT MAX("last_updated") AS max_date FROM singapore_grid_all
),
recent_grid AS (
  SELECT sg.*
  FROM singapore_grid_all sg
  CROSS JOIN max_date md
  WHERE sg."last_updated" = md.max_date
),
convex_hull AS (
  SELECT ST_CONVEXHULL(ST_COLLECT(point_geog)) AS hull
  FROM recent_grid
),
hospitals AS (
  SELECT pl."geometry" AS hospital_geom
  FROM "GEO_OPENSTREETMAP_WORLDPOP"."GEO_OPENSTREETMAP"."PLANET_LAYERS" pl
  CROSS JOIN convex_hull ch
  WHERE pl."layer_code" IN (2110, 2120)
    AND ST_INTERSECTS(pl."geometry", ch.hull)
),
grid_distances AS (
  SELECT
    rg."geo_id",
    rg."population",
    MIN(ST_DISTANCE(rg.point_geog, h.hospital_geom)) AS min_dist_to_hospital
  FROM recent_grid rg
  CROSS JOIN hospitals h
  GROUP BY rg."geo_id", rg."population"
)
SELECT "population"
FROM grid_distances
ORDER BY min_dist_to_hospital DESC
LIMIT 1