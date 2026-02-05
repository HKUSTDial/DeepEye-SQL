WITH california_roads AS (
    SELECT DISTINCT
        w."id" AS way_id,
        w."nodes" AS nodes,
        w."geometry" AS geometry
    FROM "GEO_OPENSTREETMAP_BOUNDARIES"."GEO_OPENSTREETMAP"."PLANET_WAYS" w
    CROSS JOIN "GEO_OPENSTREETMAP_BOUNDARIES"."GEO_US_BOUNDARIES"."STATES" s
    , LATERAL FLATTEN(input => w."all_tags") t
    WHERE s."state_name" = 'California'
        AND ST_INTERSECTS(TO_GEOMETRY(w."geometry"), TO_GEOMETRY(s."state_geom"))
        AND t.value:key::STRING = 'highway'
        AND t.value:value::STRING IN ('motorway', 'trunk', 'primary', 'secondary', 'residential')
        AND ARRAY_SIZE(FILTER(w."all_tags", x -> x:key::STRING = 'bridge')) = 0
),
road_pairs AS (
    SELECT 
        cr1.way_id AS way_id1,
        cr2.way_id AS way_id2
    FROM california_roads cr1
    INNER JOIN california_roads cr2
        ON cr1.way_id < cr2.way_id
    WHERE ST_INTERSECTS(TO_GEOMETRY(cr1.geometry), TO_GEOMETRY(cr2.geometry))
        AND NOT ARRAYS_OVERLAP(cr1.nodes::ARRAY, cr2.nodes::ARRAY)
)
SELECT COUNT(*) AS pair_count
FROM road_pairs