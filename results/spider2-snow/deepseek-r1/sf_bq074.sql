SELECT COUNT(DISTINCT u.county_fips)
FROM (
  SELECT a2015."geo_id" AS county_fips
  FROM "SDOH"."CENSUS_BUREAU_ACS"."COUNTY_2015_5YR" AS a2015
  INNER JOIN "SDOH"."CENSUS_BUREAU_ACS"."COUNTY_2018_5YR" AS a2018
  ON a2015."geo_id" = a2018."geo_id"
  WHERE a2018."unemployed_pop" > a2015."unemployed_pop"
) AS u
INNER JOIN (
  SELECT "FIPS" AS county_fips
  FROM "SDOH"."SDOH_CMS_DUAL_ELIGIBLE_ENROLLMENT"."DUAL_ELIGIBLE_ENROLLMENT_BY_COUNTY_AND_PROGRAM"
  WHERE "Date" IN ('2015-12-01', '2018-12-01')
  GROUP BY "FIPS"
  HAVING 
    MAX(CASE WHEN "Date" = '2015-12-01' THEN "Public_Total" END) >
    MAX(CASE WHEN "Date" = '2018-12-01' THEN "Public_Total" END)
) AS d
ON u.county_fips = d.county_fips