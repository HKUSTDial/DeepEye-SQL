SELECT
    a."country_name",
    a.under20_pop AS "total_population_under_20",
    b.total_pop AS "total_midyear_population",
    (a.under20_pop / b.total_pop) * 100 AS "percentage_under_20"
FROM (
    SELECT
        "country_code",
        "country_name",
        SUM("midyear_population") AS under20_pop
    FROM
        "CENSUS_BUREAU_INTERNATIONAL"."CENSUS_BUREAU_INTERNATIONAL"."MIDYEAR_POPULATION_5YR_AGE_SEX"
    WHERE
        "year" = 2020
        AND "total_flag" = 'A'
        AND "starting_age" < 20
    GROUP BY
        "country_code",
        "country_name"
) a
JOIN (
    SELECT
        "country_code",
        "country_name",
        "midyear_population" AS total_pop
    FROM
        "CENSUS_BUREAU_INTERNATIONAL"."CENSUS_BUREAU_INTERNATIONAL"."MIDYEAR_POPULATION"
    WHERE
        "year" = 2020
) b ON a."country_code" = b."country_code"
ORDER BY
    "percentage_under_20" DESC
LIMIT 10