WITH amsterdam_buildings AS (
    SELECT 
        b."CLASS",
        b."SUBCLASS",
        TRY_CAST(b."SURFACE_AREA_SQ_M" AS FLOAT) AS surface_area
    FROM "NETHERLANDS_OPEN_MAP_DATA"."NETHERLANDS"."V_BUILDING" b
    INNER JOIN "NETHERLANDS_OPEN_MAP_DATA"."NETHERLANDS"."V_ADMINISTRATIVE" a
        ON ST_WITHIN(b."GEO_CORDINATES", a."GEO_CORDINATES")
    CROSS JOIN LATERAL FLATTEN(INPUT => a."NAMES") names
    WHERE LOWER(names.value::STRING) LIKE '%amsterdam%'
),
rotterdam_buildings AS (
    SELECT 
        b."CLASS",
        b."SUBCLASS",
        TRY_CAST(b."SURFACE_AREA_SQ_M" AS FLOAT) AS surface_area
    FROM "NETHERLANDS_OPEN_MAP_DATA"."NETHERLANDS"."V_BUILDING" b
    INNER JOIN "NETHERLANDS_OPEN_MAP_DATA"."NETHERLANDS"."V_ADMINISTRATIVE" a
        ON ST_WITHIN(b."GEO_CORDINATES", a."GEO_CORDINATES")
    CROSS JOIN LATERAL FLATTEN(INPUT => a."NAMES") names
    WHERE LOWER(names.value::STRING) LIKE '%rotterdam%'
),
amsterdam_agg AS (
    SELECT 
        "CLASS",
        "SUBCLASS",
        SUM(surface_area) AS amsterdam_total_surface_area_sq_m,
        COUNT(*) AS amsterdam_building_count
    FROM amsterdam_buildings
    GROUP BY "CLASS", "SUBCLASS"
),
rotterdam_agg AS (
    SELECT 
        "CLASS",
        "SUBCLASS",
        SUM(surface_area) AS rotterdam_total_surface_area_sq_m,
        COUNT(*) AS rotterdam_building_count
    FROM rotterdam_buildings
    GROUP BY "CLASS", "SUBCLASS"
)
SELECT 
    COALESCE(a."CLASS", r."CLASS") AS "CLASS",
    COALESCE(a."SUBCLASS", r."SUBCLASS") AS "SUBCLASS",
    COALESCE(a.amsterdam_total_surface_area_sq_m, 0) AS "AMSTERDAM_TOTAL_SURFACE_AREA_SQ_M",
    COALESCE(a.amsterdam_building_count, 0) AS "AMSTERDAM_BUILDING_COUNT",
    COALESCE(r.rotterdam_total_surface_area_sq_m, 0) AS "ROTTERDAM_TOTAL_SURFACE_AREA_SQ_M",
    COALESCE(r.rotterdam_building_count, 0) AS "ROTTERDAM_BUILDING_COUNT"
FROM amsterdam_agg a
FULL OUTER JOIN rotterdam_agg r
    ON a."CLASS" = r."CLASS" AND a."SUBCLASS" = r."SUBCLASS"
ORDER BY COALESCE(a."CLASS", r."CLASS"), COALESCE(a."SUBCLASS", r."SUBCLASS")