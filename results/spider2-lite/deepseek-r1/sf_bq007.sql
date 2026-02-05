WITH vulnerable_state AS (
    SELECT 
        LPAD(t."geo_id", 2, '0') AS "state_fips_code",
        (COALESCE(t."employed_wholesale_trade",0) * 0.38423645320197042 +
         COALESCE(t."occupation_natural_resources_construction_maintenance",0) * 0.48071410777129553 +
         COALESCE(t."employed_arts_entertainment_recreation_accommodation_food",0) * 0.89455676291236841 +
         COALESCE(t."employed_information",0) * 0.31315240083507306 +
         COALESCE(t."employed_retail_trade",0) * 0.51 +
         COALESCE(t."employed_public_administration",0) * 0.039299298394228743 +
         COALESCE(t."occupation_services",0) * 0.36555534476489654 +
         COALESCE(t."employed_education_health_social",0) * 0.20323178400562944 +
         COALESCE(t."employed_transportation_warehousing_utilities",0) * 0.3680506593618087 +
         COALESCE(t."employed_manufacturing",0) * 0.40618955512572535) AS "vulnerable_population"
    FROM "CENSUS_BUREAU_ACS_2"."CENSUS_BUREAU_ACS"."STATE_2017_5YR" t
), income_change_state AS (
    SELECT 
        zc."state_code",
        AVG(z2018."median_income" - z2015."median_income") AS "avg_income_change"
    FROM "CENSUS_BUREAU_ACS_2"."CENSUS_BUREAU_ACS"."ZIP_CODES_2015_5YR" z2015
    JOIN "CENSUS_BUREAU_ACS_2"."CENSUS_BUREAU_ACS"."ZIP_CODES_2018_5YR" z2018
        ON z2015."geo_id" = z2018."geo_id"
    JOIN "CENSUS_BUREAU_ACS_2"."GEO_US_BOUNDARIES"."ZIP_CODES" zc
        ON z2015."geo_id" = zc."zip_code"
    WHERE z2015."median_income" IS NOT NULL AND z2018."median_income" IS NOT NULL
    GROUP BY zc."state_code"
)
SELECT 
    s."state_name",
    vs."vulnerable_population",
    COALESCE(ics."avg_income_change", 0) AS "avg_income_change"
FROM vulnerable_state vs
JOIN "CENSUS_BUREAU_ACS_2"."GEO_US_BOUNDARIES"."STATES" s 
    ON vs."state_fips_code" = s."state_fips_code"
LEFT JOIN income_change_state ics 
    ON s."state" = ics."state_code"
ORDER BY vs."vulnerable_population" DESC
LIMIT 10