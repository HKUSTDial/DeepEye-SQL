WITH philly_geom AS (
    SELECT TO_GEOGRAPHY("place_geom") AS geom
    FROM "GEO_OPENSTREETMAP_CENSUS_PLACES"."GEO_US_CENSUS_PLACES"."PLACES_PENNSYLVANIA"
    WHERE "place_name" = 'Philadelphia'
),
amenities AS (
    SELECT pf."osm_id", TO_GEOGRAPHY(pf."geometry") AS geom
    FROM "GEO_OPENSTREETMAP_CENSUS_PLACES"."GEO_OPENSTREETMAP"."PLANET_FEATURES_POINTS" AS pf
    JOIN LATERAL FLATTEN(INPUT => pf."all_tags") AS tags
    CROSS JOIN philly_geom
    WHERE tags.value:key::string = 'amenity'
      AND tags.value:value::string IN ('library', 'place_of_worship', 'community_centre')
      AND ST_CONTAINS(philly_geom.geom, TO_GEOGRAPHY(pf."geometry"))
)
SELECT MIN(ST_DISTANCE(a.geom, b.geom)) AS shortest_distance
FROM amenities a
JOIN amenities b ON a."osm_id" < b."osm_id"