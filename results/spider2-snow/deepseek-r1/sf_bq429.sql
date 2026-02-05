WITH "state_avg_diff" AS (
  SELECT 
    "zc"."state_name",
    AVG("z18"."median_income" - "z15"."median_income") AS "avg_income_diff"
  FROM "CENSUS_BUREAU_ACS_2"."CENSUS_BUREAU_ACS"."ZCTA5_2015_5YR" "z15"
  JOIN "CENSUS_BUREAU_ACS_2"."CENSUS_BUREAU_ACS"."ZCTA5_2018_5YR" "z18" 
    ON "z15"."geo_id" = "z18"."geo_id"
  JOIN "CENSUS_BUREAU_ACS_2"."GEO_US_BOUNDARIES"."ZIP_CODES" "zc" 
    ON "z15"."geo_id" = "zc"."zip_code"
  WHERE "z15"."median_income" IS NOT NULL AND "z18"."median_income" IS NOT NULL
  GROUP BY "zc"."state_name"
  ORDER BY "avg_income_diff" DESC
  LIMIT 5
),
"vulnerable_avg" AS (
  SELECT 
    "zc"."state_name",
    AVG(
      "z17"."employed_wholesale_trade" * 0.38423645320197042 +
      "z17"."occupation_natural_resources_construction_maintenance" * 0.48071410777129553 +
      "z17"."employed_arts_entertainment_recreation_accommodation_food" * 0.89455676291236841 +
      "z17"."employed_information" * 0.31315240083507306 +
      "z17"."employed_retail_trade" * 0.51
    ) AS "avg_vulnerable_employees"
  FROM "CENSUS_BUREAU_ACS_2"."CENSUS_BUREAU_ACS"."ZCTA5_2017_5YR" "z17"
  JOIN "CENSUS_BUREAU_ACS_2"."GEO_US_BOUNDARIES"."ZIP_CODES" "zc" 
    ON "z17"."geo_id" = "zc"."zip_code"
  WHERE 
    "z17"."employed_wholesale_trade" IS NOT NULL
    AND "z17"."occupation_natural_resources_construction_maintenance" IS NOT NULL
    AND "z17"."employed_arts_entertainment_recreation_accommodation_food" IS NOT NULL
    AND "z17"."employed_information" IS NOT NULL
    AND "z17"."employed_retail_trade" IS NOT NULL
    AND "zc"."state_name" IN (SELECT "state_name" FROM "state_avg_diff")
  GROUP BY "zc"."state_name"
)
SELECT 
  "state_avg_diff"."state_name",
  "vulnerable_avg"."avg_vulnerable_employees"
FROM "state_avg_diff"
JOIN "vulnerable_avg" ON "state_avg_diff"."state_name" = "vulnerable_avg"."state_name"
ORDER BY "state_avg_diff"."avg_income_diff" DESC