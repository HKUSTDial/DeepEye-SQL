WITH CONFIRMED_DATA AS (
  SELECT 
    "country_or_region" AS "country",
    TRY_CAST("_4_20_20" AS NUMBER) AS "confirmed_cases"
  FROM 
    "COVID19_JHU_WORLD_BANK"."COVID19_JHU_CSSE"."CONFIRMED_CASES"
  WHERE 
    "country_or_region" IN ('United States', 'France', 'China', 'Italy', 'Spain', 'Germany', 'Iran')
),
POPULATION_DATA AS (
  SELECT 
    "country",
    COUNT(*) AS "population"
  FROM 
    "COVID19_JHU_WORLD_BANK"."WORLD_BANK_GLOBAL_POPULATION"."POPULATION_BY_COUNTRY"
  WHERE 
    "country" IN ('United States', 'France', 'China', 'Italy', 'Spain', 'Germany', 'Iran')
  GROUP BY 
    "country"
)
SELECT 
  CD."country",
  CD."confirmed_cases",
  PD."population",
  ROUND((CD."confirmed_cases" / PD."population") * 100000, 2) AS "cases_per_100k"
FROM 
  CONFIRMED_DATA CD
INNER JOIN 
  POPULATION_DATA PD
  ON CD."country" = PD."country"
ORDER BY 
  CD."confirmed_cases" DESC