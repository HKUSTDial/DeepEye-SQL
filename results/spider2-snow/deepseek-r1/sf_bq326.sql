WITH "pop_increase" AS (
  SELECT DISTINCT "country_code"
  FROM "WORLD_BANK"."WORLD_BANK_GLOBAL_POPULATION"."POPULATION_BY_COUNTRY"
  WHERE ("year_2018" - "year_2017") / "year_2017" > 0.01
),
"series_code" AS (
  SELECT "series_code"
  FROM "WORLD_BANK"."WORLD_BANK_HEALTH_POPULATION"."SERIES_SUMMARY"
  WHERE "indicator_name" ILIKE '%current health expenditure per capita%' AND "indicator_name" ILIKE '%PPP%'
  LIMIT 1
),
"health_data" AS (
  SELECT "country_code", "year", "value"
  FROM "WORLD_BANK"."WORLD_BANK_HEALTH_POPULATION"."HEALTH_NUTRITION_POPULATION"
  WHERE "indicator_code" = (SELECT "series_code" FROM "series_code")
    AND "year" IN (2017, 2018)
),
"health_aggregated" AS (
  SELECT "country_code",
    MAX(CASE WHEN "year" = 2018 THEN "value" END) AS "health_2018",
    MAX(CASE WHEN "year" = 2017 THEN "value" END) AS "health_2017"
  FROM "health_data"
  GROUP BY "country_code"
  HAVING "health_2018" IS NOT NULL AND "health_2017" IS NOT NULL
),
"health_increase" AS (
  SELECT "country_code"
  FROM "health_aggregated"
  WHERE ("health_2018" - "health_2017") / "health_2017" > 0.01
)
SELECT COUNT(DISTINCT "p"."country_code") AS "country_count"
FROM "pop_increase" AS "p"
INNER JOIN "health_increase" AS "h" ON "p"."country_code" = "h"."country_code"