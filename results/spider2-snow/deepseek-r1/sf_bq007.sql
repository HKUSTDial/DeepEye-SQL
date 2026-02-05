WITH state_vulnerable AS (
  SELECT 
    s."geo_id" AS state_fips,
    (s."employed_wholesale_trade" * 0.38423645320197042 +
     s."occupation_natural_resources_construction_maintenance" * 0.48071410777129553 +
     s."employed_arts_entertainment_recreation_accommodation_food" * 0.89455676291236841 +
     s."employed_information" * 0.31315240083507306 +
     s."employed_retail_trade" * 0.51 +
     s."employed_public_administration" * 0.039299298394228743 +
     s."occupation_services" * 0.36555534476489654 +
     s."employed_education_health_social" * 0.20323178400562944 +
     s."employed_transportation_warehousing_utilities" * 0.3680506593618087 +
     s."employed_manufacturing" * 0.40618955512572535) AS vulnerable_population
  FROM "CENSUS_BUREAU_ACS_2"."CENSUS_BUREAU_ACS"."STATE_2017_5YR" s
),
zip_income_change AS (
  SELECT 
    zc."state_code",
    AVG(z2018."median_income" - z2015."median_income") AS avg_median_income_change
  FROM "CENSUS_BUREAU_ACS_2"."CENSUS_BUREAU_ACS"."ZIP_CODES_2015_5YR" z2015
  INNER JOIN "CENSUS_BUREAU_ACS_2"."CENSUS_BUREAU_ACS"."ZIP_CODES_2018_5YR" z2018 
    ON z2015."geo_id" = z2018."geo_id"
  INNER JOIN "CENSUS_BUREAU_ACS_2"."GEO_US_BOUNDARIES"."ZIP_CODES" zc 
    ON z2015."geo_id" = zc."zip_code"
  GROUP BY zc."state_code"
)
SELECT 
  st."state_name",
  sv.vulnerable_population,
  zic.avg_median_income_change
FROM state_vulnerable sv
INNER JOIN "CENSUS_BUREAU_ACS_2"."GEO_US_BOUNDARIES"."STATES" st 
  ON sv.state_fips = st."geo_id"
INNER JOIN zip_income_change zic 
  ON st."state" = zic."state_code"
ORDER BY sv.vulnerable_population DESC
LIMIT 10