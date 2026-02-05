WITH t2017 AS (
    SELECT 
        "geo_id",
        "total_pop",
        GREATEST("unemployed_pop" + "not_in_labor_force" - "group_quarters", 0) AS "adjusted_non_labor",
        SUBSTR("geo_id", 1, 2) AS "state_fips_str"
    FROM "CENSUS_BUREAU_ACS_2"."CENSUS_BUREAU_ACS"."CENSUSTRACT_2017_5YR"
),
t2015 AS (
    SELECT "geo_id", "median_income" AS "median_income_2015"
    FROM "CENSUS_BUREAU_ACS_2"."CENSUS_BUREAU_ACS"."CENSUSTRACT_2015_5YR"
),
t2018 AS (
    SELECT "geo_id", "median_income" AS "median_income_2018"
    FROM "CENSUS_BUREAU_ACS_2"."CENSUS_BUREAU_ACS"."CENSUSTRACT_2018_5YR"
),
joined_tracts AS (
    SELECT 
        t2017."geo_id",
        t2017."state_fips_str",
        t2017."total_pop",
        t2017."adjusted_non_labor",
        t2015."median_income_2015",
        t2018."median_income_2018"
    FROM t2017
    INNER JOIN t2015 ON t2017."geo_id" = t2015."geo_id"
    INNER JOIN t2018 ON t2017."geo_id" = t2018."geo_id"
),
tracts_with_state AS (
    SELECT 
        j.*,
        sf."postal_code",
        (j."median_income_2018" - j."median_income_2015") AS "income_change",
        j."adjusted_non_labor" / NULLIF(j."total_pop", 0) AS "ratio"
    FROM joined_tracts j
    INNER JOIN "CENSUS_BUREAU_ACS_2"."CYCLISTIC"."STATE_FIPS" sf
        ON TO_NUMBER(j."state_fips_str") = sf."fips"
)
SELECT 
    "postal_code" AS "state_abbr",
    SUM("income_change") AS "total_median_income_change",
    SUM("adjusted_non_labor") AS "total_adjusted_non_labor",
    AVG("ratio") AS "avg_population_adjusted_proportion"
FROM tracts_with_state
GROUP BY "postal_code"
ORDER BY "total_adjusted_non_labor" ASC
LIMIT 3