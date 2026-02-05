WITH gdp_indicator AS (
  SELECT "series_code"
  FROM "WORLD_BANK"."WORLD_BANK_WDI"."SERIES_SUMMARY"
  WHERE "indicator_name" ILIKE '%GDP (constant 2015 US$)%'
  LIMIT 1
),
gdp_data AS (
  SELECT "country_code", "year", "value"
  FROM "WORLD_BANK"."WORLD_BANK_WDI"."INDICATORS_DATA"
  WHERE "indicator_code" = (SELECT "series_code" FROM gdp_indicator)
),
latest_gdp AS (
  SELECT "country_code", "value" AS latest_gdp_value
  FROM (
    SELECT "country_code", "value", ROW_NUMBER() OVER (PARTITION BY "country_code" ORDER BY "year" DESC) AS rn
    FROM gdp_data
  ) ranked
  WHERE rn = 1
)
SELECT cs."region"
FROM latest_gdp lg
INNER JOIN "WORLD_BANK"."WORLD_BANK_WDI"."COUNTRY_SUMMARY" cs ON lg."country_code" = cs."country_code"
WHERE cs."region" IS NOT NULL
GROUP BY cs."region"
ORDER BY MEDIAN(lg.latest_gdp_value) DESC
LIMIT 1