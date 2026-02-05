WITH "total_pop" AS (
    SELECT "country_code", "country_name", "midyear_population" AS "total_pop"
    FROM "CENSUS_BUREAU_INTERNATIONAL"."CENSUS_BUREAU_INTERNATIONAL"."MIDYEAR_POPULATION"
    WHERE "year" = 2017
),
"under25_pop" AS (
    SELECT "country_code", "country_name", SUM("midyear_population") AS "under25_pop"
    FROM "CENSUS_BUREAU_INTERNATIONAL"."CENSUS_BUREAU_INTERNATIONAL"."MIDYEAR_POPULATION_5YR_AGE_SEX"
    WHERE "year" = 2017 AND "total_flag" = 'A' AND "starting_age" < 25
    GROUP BY "country_code", "country_name"
)
SELECT 
    "t"."country_name",
    ("u"."under25_pop" / "t"."total_pop") * 100 AS "percentage_under25"
FROM "total_pop" AS "t"
JOIN "under25_pop" AS "u" ON "t"."country_code" = "u"."country_code"
ORDER BY "percentage_under25" DESC
LIMIT 1