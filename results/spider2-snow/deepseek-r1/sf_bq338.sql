WITH tract_data AS (
    SELECT 
        t11."geo_id" AS geo_id,
        t11."total_pop" AS pop_2011,
        t18."total_pop" AS pop_2018,
        t11."median_income" AS income_2011,
        t18."median_income" AS income_2018,
        (t18."total_pop" - t11."total_pop") / t11."total_pop" * 100 AS pop_increase_pct,
        t18."median_income" - t11."median_income" AS income_increase_abs
    FROM 
        "CENSUS_BUREAU_ACS_1"."CENSUS_BUREAU_ACS"."CENSUSTRACT_2011_5YR" AS t11
    INNER JOIN 
        "CENSUS_BUREAU_ACS_1"."CENSUS_BUREAU_ACS"."CENSUSTRACT_2018_5YR" AS t18
        ON t11."geo_id" = t18."geo_id"
    WHERE 
        t11."geo_id" LIKE '36047%'
        AND t11."total_pop" > 1000
        AND t18."total_pop" > 1000
),
ranked AS (
    SELECT 
        geo_id,
        pop_increase_pct,
        income_increase_abs,
        RANK() OVER (ORDER BY pop_increase_pct DESC) AS rank_pop,
        RANK() OVER (ORDER BY income_increase_abs DESC) AS rank_income
    FROM tract_data
)
SELECT 
    geo_id
FROM ranked
WHERE rank_pop <= 20 AND rank_income <= 20