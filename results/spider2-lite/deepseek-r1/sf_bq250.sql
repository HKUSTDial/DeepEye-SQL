WITH latest_singapore_grid AS (
    SELECT *
    FROM "GEO_OPENSTREETMAP_WORLDPOP"."WORLDPOP"."POPULATION_GRID_1KM"
    WHERE "country_name" = 'Singapore' 
      AND "last_updated" < '2023-01-01'
      AND "last_updated" = (
          SELECT MAX("last_updated") 
          FROM "GEO_OPENSTREETMAP_WORLDPOP"."WORLDPOP"."POPULATION_GRID_1KM"
          WHERE "country_name" = 'Singapore' 
            AND "last_updated" < '2023-01-01'
      )
),
convex_hull_region AS (
    SELECT ST_ENVELOPE(ST_COLLECT(ST_POINT("longitude_centroid", "latitude_centroid"))) AS hull
    FROM latest_singapore_grid
),
hospitals_in_region AS (
    SELECT TO_GEOGRAPHY("geometry") AS hospital_geom
    FROM "GEO_OPENSTREETMAP_WORLDPOP"."GEO_OPENSTREETMAP"."PLANET_LAYERS"
    WHERE "layer_code" IN (2110, 2120)
      AND ST_INTERSECTS(TO_GEOGRAPHY("geometry"), (SELECT hull FROM convex_hull_region))
),
grid_distances AS (
    SELECT 
        g."geo_id", 
        g."population", 
        g."longitude_centroid", 
        g."latitude_centroid",
        MIN(ST_DISTANCE(ST_POINT(g."longitude_centroid", g."latitude_centroid"), h.hospital_geom)) AS min_distance
    FROM latest_singapore_grid g
    CROSS JOIN hospitals_in_region h
    GROUP BY g."geo_id", g."population", g."longitude_centroid", g."latitude_centroid"
),
farthest_grid_cell AS (
    SELECT *
    FROM grid_distances
    ORDER BY min_distance DESC
    LIMIT 1
)
SELECT "population"
FROM farthest_grid_cell