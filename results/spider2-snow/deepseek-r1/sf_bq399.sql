WITH birth_rate_series AS (
  SELECT "series_code"
  FROM "WORLD_BANK"."WORLD_BANK_WDI"."SERIES_SUMMARY"
  WHERE "indicator_name" ILIKE '%crude birth%' OR "indicator_name" ILIKE '%birth rate, crude%'
  LIMIT 1
),
birth_rate_data AS (
  SELECT "country_code", "year", "value"
  FROM "WORLD_BANK"."WORLD_BANK_WDI"."INDICATORS_DATA"
  INNER JOIN birth_rate_series ON "indicator_code" = "series_code"
  WHERE "year" BETWEEN 1980 AND 1989
),
high_income_countries AS (
  SELECT "country_code", "region", "short_name" AS "country_name"
  FROM "WORLD_BANK"."WORLD_BANK_WDI"."COUNTRY_SUMMARY"
  WHERE "income_group" = 'High income' AND "region" IS NOT NULL
),
avg_birth_rates AS (
  SELECT hc."region", hc."country_code", hc."country_name", AVG(brd."value") AS "avg_birth_rate"
  FROM birth_rate_data brd
  INNER JOIN high_income_countries hc ON brd."country_code" = hc."country_code"
  GROUP BY hc."region", hc."country_code", hc."country_name"
),
ranked AS (
  SELECT "region", "country_name", "avg_birth_rate", RANK() OVER (PARTITION BY "region" ORDER BY "avg_birth_rate" DESC) AS "rank"
  FROM avg_birth_rates
)
SELECT "region", "country_name", "avg_birth_rate"
FROM ranked
WHERE "rank" = 1
ORDER BY "region"