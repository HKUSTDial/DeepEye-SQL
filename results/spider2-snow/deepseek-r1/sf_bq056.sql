WITH california_roads AS (
  SELECT 
    ways."id" AS id,
    ways."nodes" AS nodes,
    TO_GEOMETRY(ways."geometry") AS geometry
  FROM 
    "GEO_OPENSTREETMAP_BOUNDARIES"."GEO_OPENSTREETMAP"."PLANET_WAYS" AS ways
  INNER JOIN 
    "GEO_OPENSTREETMAP_BOUNDARIES"."GEO_US_BOUNDARIES"."STATES" AS states
    ON ST_INTERSECTS(TO_GEOMETRY(ways."geometry"), TO_GEOMETRY(states."state_geom"))
  WHERE 
    states."state" = 'CA'
    AND ways."all_tags"['highway'] IN ('motorway', 'trunk', 'primary', 'secondary', 'residential')
    AND ways."all_tags"['bridge'] IS NULL
    AND ways."visible" = TRUE
)
SELECT 
  COUNT(*) AS pair_count
FROM 
  california_roads r1
JOIN 
  california_roads r2
  ON r1.id < r2.id
  AND ST_INTERSECTS(r1.geometry, r2.geometry)
  AND NOT ARRAYS_OVERLAP(r1.nodes::ARRAY, r2.nodes::ARRAY)