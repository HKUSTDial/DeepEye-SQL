WITH utah_counties AS (
  SELECT county_fips_code, county_name
  FROM `bigquery-public-data.geo_us_boundaries.counties`
  WHERE state_fips_code = '49'
),
employment_2000 AS (
  SELECT c.county_fips_code, AVG(b.month3_emplvl_23_construction) AS avg_2000
  FROM `bigquery-public-data.bls_qcew.2000_*` AS b
  INNER JOIN utah_counties c ON b.area_fips = c.county_fips_code
  WHERE _TABLE_SUFFIX IN ('q1','q2','q3','q4')
  GROUP BY c.county_fips_code
),
employment_2018 AS (
  SELECT c.county_fips_code, AVG(b.month3_emplvl_23_construction) AS avg_2018
  FROM `bigquery-public-data.bls_qcew.2018_*` AS b
  INNER JOIN utah_counties c ON b.area_fips = c.county_fips_code
  WHERE _TABLE_SUFFIX IN ('q1','q2','q3','q4')
  GROUP BY c.county_fips_code
)
SELECT 
  c.county_name,
  (e2018.avg_2018 - e2000.avg_2000) / e2000.avg_2000 * 100 AS pct_increase
FROM employment_2000 e2000
INNER JOIN employment_2018 e2018 ON e2000.county_fips_code = e2018.county_fips_code
INNER JOIN utah_counties c ON e2000.county_fips_code = c.county_fips_code
WHERE e2000.avg_2000 > 0
ORDER BY pct_increase DESC
LIMIT 1