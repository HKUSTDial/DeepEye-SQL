SELECT 
  s."state_name",
  0.38 * e."employed_wholesale_trade" AS "vulnerable_wholesale_workers",
  0.41 * e."employed_manufacturing" AS "vulnerable_manufacturing_workers",
  (0.38 * e."employed_wholesale_trade" + 0.41 * e."employed_manufacturing") AS "total_vulnerable_workers"
FROM 
  "CENSUS_BUREAU_ACS_2"."CENSUS_BUREAU_ACS"."STATE_2017_1YR" e
JOIN 
  "CENSUS_BUREAU_ACS_2"."GEO_US_BOUNDARIES"."STATES" s
ON 
  e."geo_id" = s."state_fips_code"
ORDER BY 
  "total_vulnerable_workers" DESC