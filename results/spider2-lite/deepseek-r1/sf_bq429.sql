WITH income_diff AS (
    SELECT 
        zc."state_name" AS state,
        z2015."geo_id" AS zip,
        z2018."median_income" - z2015."median_income" AS income_diff
    FROM "CENSUS_BUREAU_ACS_2"."CENSUS_BUREAU_ACS"."ZCTA5_2015_5YR" z2015
    INNER JOIN "CENSUS_BUREAU_ACS_2"."CENSUS_BUREAU_ACS"."ZCTA5_2018_5YR" z2018 
        ON z2015."geo_id" = z2018."geo_id"
    INNER JOIN "CENSUS_BUREAU_ACS_2"."GEO_US_BOUNDARIES"."ZIP_CODES" zc 
        ON z2015."geo_id" = zc."zip_code"
),
state_avg_diff AS (
    SELECT 
        state,
        AVG(income_diff) AS avg_income_diff
    FROM income_diff
    GROUP BY state
    ORDER BY avg_income_diff DESC
    LIMIT 5
),
vulnerable_2017 AS (
    SELECT 
        zc."state_name" AS state,
        (z2017."employed_wholesale_trade" * 0.38423645320197042 
         + (z2017."employed_agriculture_forestry_fishing_hunting_mining" + z2017."employed_construction") * 0.48071410777129553
         + z2017."employed_arts_entertainment_recreation_accommodation_food" * 0.89455676291236841
         + z2017."employed_information" * 0.31315240083507306
         + z2017."employed_retail_trade" * 0.51) AS vulnerable_employees
    FROM "CENSUS_BUREAU_ACS_2"."CENSUS_BUREAU_ACS"."ZCTA5_2017_5YR" z2017
    INNER JOIN "CENSUS_BUREAU_ACS_2"."GEO_US_BOUNDARIES"."ZIP_CODES" zc 
        ON z2017."geo_id" = zc."zip_code"
    WHERE zc."state_name" IN (SELECT state FROM state_avg_diff)
),
state_vulnerable AS (
    SELECT 
        state,
        AVG(vulnerable_employees) AS avg_vulnerable_employees
    FROM vulnerable_2017
    GROUP BY state
)
SELECT 
    sad.state,
    sad.avg_income_diff,
    sv.avg_vulnerable_employees
FROM state_avg_diff sad
LEFT JOIN state_vulnerable sv ON sad.state = sv.state
ORDER BY sad.avg_income_diff DESC