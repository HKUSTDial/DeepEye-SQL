WITH tract_2017 AS (
    SELECT 
        "geo_id",
        LEFT("geo_id", 2) AS state_fips_code,
        GREATEST("unemployed_pop" + "not_in_labor_force" - "group_quarters", 0) AS adjusted_non_labor,
        "total_pop",
        CASE WHEN "total_pop" > 0 THEN adjusted_non_labor / "total_pop" ELSE NULL END AS proportion
    FROM "CENSUS_BUREAU_ACS_2"."CENSUS_BUREAU_ACS"."CENSUSTRACT_2017_5YR"
),
tract_2015 AS (
    SELECT "geo_id", "median_income" AS median_income_2015
    FROM "CENSUS_BUREAU_ACS_2"."CENSUS_BUREAU_ACS"."CENSUSTRACT_2015_5YR"
),
tract_2018 AS (
    SELECT "geo_id", "median_income" AS median_income_2018
    FROM "CENSUS_BUREAU_ACS_2"."CENSUS_BUREAU_ACS"."CENSUSTRACT_2018_5YR"
),
joined_tracts AS (
    SELECT 
        t17."geo_id",
        t17.state_fips_code,
        t17.adjusted_non_labor,
        t17.proportion,
        t15.median_income_2015,
        t18.median_income_2018,
        t18.median_income_2018 - t15.median_income_2015 AS income_diff
    FROM tract_2017 t17
    INNER JOIN tract_2015 t15 ON t17."geo_id" = t15."geo_id"
    INNER JOIN tract_2018 t18 ON t17."geo_id" = t18."geo_id"
    WHERE t15.median_income_2015 IS NOT NULL AND t18.median_income_2018 IS NOT NULL
),
state_aggregates AS (
    SELECT 
        s."state" AS state_abbr,
        SUM(jt.adjusted_non_labor) AS total_adjusted_non_labor,
        SUM(jt.income_diff) AS total_median_income_change,
        AVG(jt.proportion) AS average_population_adjusted_proportion
    FROM joined_tracts jt
    INNER JOIN "CENSUS_BUREAU_ACS_2"."GEO_US_BOUNDARIES"."STATES" s ON jt.state_fips_code = s."state_fips_code"
    GROUP BY s."state"
)
SELECT 
    state_abbr,
    total_median_income_change,
    total_adjusted_non_labor,
    average_population_adjusted_proportion
FROM state_aggregates
ORDER BY total_adjusted_non_labor ASC
LIMIT 3