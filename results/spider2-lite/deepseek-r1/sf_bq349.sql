WITH administrative_boundaries AS (
    SELECT DISTINCT
        "osm_way_id",
        TO_GEOGRAPHY("geometry") AS boundary_geog
    FROM "GEO_OPENSTREETMAP"."GEO_OPENSTREETMAP"."PLANET_FEATURES"
    INNER JOIN LATERAL FLATTEN(INPUT => "all_tags") AS tags
    WHERE "feature_type" = 'multipolygons'
        AND tags.value:"key"::STRING = 'boundary'
        AND tags.value:"value"::STRING = 'administrative'
),
amenity_nodes AS (
    SELECT DISTINCT
        "id",
        TO_GEOGRAPHY("geometry") AS node_geog
    FROM "GEO_OPENSTREETMAP"."GEO_OPENSTREETMAP"."PLANET_NODES"
    INNER JOIN LATERAL FLATTEN(INPUT => "all_tags") AS tags
    WHERE "visible" = TRUE
        AND tags.value:"key"::STRING = 'amenity'
        AND tags.value:"value"::STRING IS NOT NULL
),
boundary_counts AS (
    SELECT
        ab."osm_way_id",
        COUNT(DISTINCT an."id") AS amenity_count
    FROM administrative_boundaries ab
    LEFT JOIN amenity_nodes an
        ON ST_COVERS(ab.boundary_geog, an.node_geog)
    GROUP BY ab."osm_way_id"
),
median_value AS (
    SELECT MEDIAN(amenity_count) AS median_count
    FROM boundary_counts
)
SELECT
    bc."osm_way_id"
FROM boundary_counts bc
CROSS JOIN median_value mv
ORDER BY ABS(bc.amenity_count - mv.median_count)
LIMIT 1