WITH zip_data AS (
    SELECT 
        z2017."geo_id",
        z2017."employed_manufacturing",
        z2017."employed_wholesale_trade",
        z2015."median_income" AS median_income_2015,
        z2018."median_income" AS median_income_2018,
        0.41 * z2017."employed_manufacturing" AS vulnerable_manufacturing,
        0.38 * z2017."employed_wholesale_trade" AS vulnerable_wholesale
    FROM 
        "CENSUS_BUREAU_ACS_2"."CENSUS_BUREAU_ACS"."ZCTA5_2017_5YR" z2017
        INNER JOIN "CENSUS_BUREAU_ACS_2"."CENSUS_BUREAU_ACS"."ZCTA5_2015_5YR" z2015 
            ON z2017."geo_id" = z2015."geo_id"
        INNER JOIN "CENSUS_BUREAU_ACS_2"."CENSUS_BUREAU_ACS"."ZCTA5_2018_5YR" z2018 
            ON z2017."geo_id" = z2018."geo_id"
    WHERE 
        z2018."median_income" < z2015."median_income"
        AND z2017."employed_manufacturing" IS NOT NULL
        AND z2017."employed_wholesale_trade" IS NOT NULL
),
zip_state AS (
    SELECT 
        zd."geo_id",
        zc."state_name" AS "state_name",
        zd.vulnerable_manufacturing,
        zd.vulnerable_wholesale
    FROM zip_data zd
    INNER JOIN "CENSUS_BUREAU_ACS_2"."GEO_US_BOUNDARIES"."ZIP_CODES" zc
        ON zd."geo_id" = zc."zip_code"
)
SELECT 
    "state_name",
    SUM(vulnerable_wholesale) AS vulnerable_wholesale_workers,
    SUM(vulnerable_manufacturing) AS vulnerable_manufacturing_workers,
    SUM(vulnerable_wholesale + vulnerable_manufacturing) AS total_vulnerable_workers
FROM zip_state
GROUP BY "state_name"
ORDER BY total_vulnerable_workers DESC