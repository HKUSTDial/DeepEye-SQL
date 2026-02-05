WITH tract_data AS (
    SELECT 
        c."geo_id",
        c."total_pop",
        c."income_per_capita",
        TO_GEOGRAPHY(t."tract_geom") AS "tract_geog"
    FROM "CENSUS_BUREAU_ACS_1"."CENSUS_BUREAU_ACS"."CENSUSTRACT_2017_5YR" c
    JOIN "CENSUS_BUREAU_ACS_1"."GEO_CENSUS_TRACTS"."US_CENSUS_TRACTS_NATIONAL" t
        ON c."geo_id" = t."geo_id"
    WHERE c."total_pop" > 0 AND c."income_per_capita" IS NOT NULL
),
zip_codes_in_radius AS (
    SELECT 
        z."zip_code",
        TO_GEOGRAPHY(z."zip_code_geom") AS "zip_geog"
    FROM "CENSUS_BUREAU_ACS_1"."GEO_US_BOUNDARIES"."ZIP_CODES" z
    WHERE ST_DWITHIN(
        TO_GEOGRAPHY(z."zip_code_geom"),
        ST_MAKEPOINT(-122.191667, 47.685833),
        8046.72
    )
),
intersections AS (
    SELECT 
        t."geo_id",
        t."total_pop",
        t."income_per_capita",
        z."zip_code",
        ST_AREA(t."tract_geog") AS "tract_area",
        ST_AREA(ST_INTERSECTION(t."tract_geog", z."zip_geog")) AS "overlap_area"
    FROM tract_data t
    JOIN zip_codes_in_radius z
        ON ST_INTERSECTS(t."tract_geog", z."zip_geog")
    WHERE ST_AREA(t."tract_geog") > 0
),
allocations AS (
    SELECT 
        "zip_code",
        ("overlap_area" / "tract_area") * "total_pop" AS "allocated_pop",
        ("overlap_area" / "tract_area") * "total_pop" * "income_per_capita" AS "allocated_income"
    FROM intersections
    WHERE "tract_area" > 0 AND "overlap_area" > 0
)
SELECT 
    "zip_code",
    ROUND(SUM("allocated_pop"), 1) AS "total_population",
    ROUND(SUM("allocated_income") / NULLIF(SUM("allocated_pop"), 0), 1) AS "avg_individual_income"
FROM allocations
GROUP BY "zip_code"
HAVING SUM("allocated_pop") > 0
ORDER BY "avg_individual_income" DESC