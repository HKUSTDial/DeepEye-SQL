WITH utah_zips AS (
  SELECT zip_code
  FROM `bigquery-public-data.geo_us_boundaries.zip_codes`
  WHERE state_name = 'Utah'
),
location_counts AS (
  SELECT u.zip_code, COUNT(i.fdic_certificate_number) AS loc_count
  FROM utah_zips u
  LEFT JOIN `bigquery-public-data.fdic_banks.locations` l ON u.zip_code = l.zip_code
  LEFT JOIN `bigquery-public-data.fdic_banks.institutions` i ON l.fdic_certificate_number = i.fdic_certificate_number AND i.fdic_insured = TRUE
  GROUP BY u.zip_code
),
min_location_zip AS (
  SELECT zip_code
  FROM location_counts
  WHERE loc_count = (SELECT MIN(loc_count) FROM location_counts)
)
SELECT m.zip_code, acs.employed_population_16_and_over / NULLIF(acs.population_16_and_over, 0) AS employment_rate
FROM min_location_zip m
JOIN `bigquery-public-data.census_bureau_acs.zip_codes_2017_5yr` acs ON m.zip_code = acs.zip_code