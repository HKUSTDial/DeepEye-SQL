WITH philly AS (
  SELECT TO_GEOGRAPHY("place_geom") AS philly_geo
  FROM "GEO_OPENSTREETMAP_CENSUS_PLACES"."GEO_US_CENSUS_PLACES"."PLACES_PENNSYLVANIA"
  WHERE "place_name" = 'Philadelphia'
),
amenities AS (
  SELECT DISTINCT p."osm_id", TO_GEOGRAPHY(p."geometry") AS point_geo
  FROM "GEO_OPENSTREETMAP_CENSUS_PLACES"."GEO_OPENSTREETMAP"."PLANET_FEATURES_POINTS" p
  CROSS JOIN LATERAL FLATTEN(INPUT => p."all_tags") tags
  INNER JOIN philly ON ST_CONTAINS(philly.philly_geo, TO_GEOGRAPHY(p."geometry"))
  WHERE tags."VALUE":"key"::STRING = 'amenity' 
    AND tags."VALUE":"value"::STRING IN ('library', 'place_of_worship', 'community_center')
)
SELECT MIN(ST_DISTANCE(a.point_geo, b.point_geo)) AS shortest_distance
FROM amenities a
JOIN amenities b ON a."osm_id" < b."osm_id"