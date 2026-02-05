WITH california_population AS (
    SELECT 
        "geo_id",
        "total_pop"
    FROM "COVID19_USA"."CENSUS_BUREAU_ACS"."COUNTY_2018_5YR"
    WHERE "geo_id" LIKE '06%'
),
vaccine_sites AS (
    SELECT 
        "facility_sub_region_2_code",
        COUNT(DISTINCT "facility_provider_id") AS "site_count"
    FROM "COVID19_USA"."COVID19_VACCINATION_ACCESS"."FACILITY_BOUNDARY_US_ALL"
    WHERE "facility_sub_region_1_code" = 'US-CA'
    GROUP BY "facility_sub_region_2_code"
)
SELECT 
    cp."geo_id" AS "county_fips",
    cp."total_pop",
    COALESCE(vs."site_count", 0) AS "site_count",
    (COALESCE(vs."site_count", 0) / cp."total_pop") * 1000 AS "sites_per_1000"
FROM california_population cp
LEFT JOIN vaccine_sites vs ON cp."geo_id" = vs."facility_sub_region_2_code"
ORDER BY cp."geo_id"