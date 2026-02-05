SELECT 
    covid."county_name",
    covid."state",
    census."median_age",
    census."total_pop",
    (covid."confirmed_cases" / census."total_pop") * 100000 AS confirmed_cases_per_100k,
    (covid."deaths" / census."total_pop") * 100000 AS deaths_per_100k,
    (covid."deaths" / covid."confirmed_cases") * 100 AS case_fatality_rate_percentage
FROM "COVID19_USA"."COVID19_USAFACTS"."SUMMARY" covid
JOIN "COVID19_USA"."CENSUS_BUREAU_ACS"."COUNTY_2020_5YR" census
    ON covid."county_fips_code" = census."geo_id"
WHERE covid."date" = '2020-08-27'
    AND census."total_pop" > 50000
    AND covid."confirmed_cases" > 0
ORDER BY case_fatality_rate_percentage DESC
LIMIT 3