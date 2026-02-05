SELECT 
    pop."country" AS country_name,
    pop."year_2018" AS population_2018,
    covid.cumulative_confirmed,
    (covid.cumulative_confirmed / pop."year_2018") * 100 AS percent_confirmed
FROM 
    "COVID19_OPEN_WORLD_BANK"."WORLD_BANK_GLOBAL_POPULATION"."POPULATION_BY_COUNTRY" AS pop
INNER JOIN
    (SELECT 
         "iso_3166_1_alpha_3" AS country_code,
         MAX("cumulative_confirmed") AS cumulative_confirmed
     FROM 
         "COVID19_OPEN_WORLD_BANK"."COVID19_OPEN_DATA"."COVID19_OPEN_DATA"
     WHERE 
         "aggregation_level" = 0 
         AND "date" <= '2020-06-30'
         AND "cumulative_confirmed" IS NOT NULL
     GROUP BY 
         "iso_3166_1_alpha_3") AS covid
ON pop."country_code" = covid.country_code
WHERE 
    pop."year_2018" IS NOT NULL
ORDER BY 
    pop."country"