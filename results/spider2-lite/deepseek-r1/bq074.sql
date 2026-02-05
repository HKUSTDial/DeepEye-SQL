WITH unemployment_2015 AS (
  SELECT geo_id, unemployed_pop 
  FROM `bigquery-public-data.census_bureau_acs.county_2015_5yr`
),
unemployment_2018 AS (
  SELECT geo_id, unemployed_pop 
  FROM `bigquery-public-data.census_bureau_acs.county_2018_5yr`
),
enrollment_2015 AS (
  SELECT FIPS, Public_Total 
  FROM `bigquery-public-data.sdoh_cms_dual_eligible_enrollment.dual_eligible_enrollment_by_county_and_program`
  WHERE Date = '2015-12-01'
),
enrollment_2018 AS (
  SELECT FIPS, Public_Total 
  FROM `bigquery-public-data.sdoh_cms_dual_eligible_enrollment.dual_eligible_enrollment_by_county_and_program`
  WHERE Date = '2018-12-01'
)
SELECT COUNT(DISTINCT u15.geo_id) AS county_count
FROM unemployment_2015 u15
INNER JOIN unemployment_2018 u18 ON u15.geo_id = u18.geo_id
INNER JOIN enrollment_2015 e15 ON u15.geo_id = e15.FIPS
INNER JOIN enrollment_2018 e18 ON u15.geo_id = e18.FIPS
WHERE u18.unemployed_pop > u15.unemployed_pop
  AND e18.Public_Total < e15.Public_Total