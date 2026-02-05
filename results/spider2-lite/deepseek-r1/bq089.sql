WITH county_population AS (
  SELECT geo_id, total_pop
  FROM `bigquery-public-data.census_bureau_acs.county_2018_5yr`
  WHERE LEFT(geo_id, 2) = '06'
), vaccine_sites_count AS (
  SELECT facility_sub_region_2_code, COUNT(DISTINCT facility_place_id) as site_count
  FROM `bigquery-public-data.covid19_vaccination_access.facility_boundary_us_all`
  WHERE facility_sub_region_1_code = 'US-CA'
  GROUP BY facility_sub_region_2_code
)
SELECT 
  cp.geo_id as county_fips,
  cp.total_pop as population,
  COALESCE(vsc.site_count, 0) as vaccine_site_count,
  CASE 
    WHEN cp.total_pop > 0 THEN (COALESCE(vsc.site_count, 0) * 1000.0) / cp.total_pop
    ELSE 0
  END as sites_per_1000_people
FROM county_population cp
LEFT JOIN vaccine_sites_count vsc ON cp.geo_id = vsc.facility_sub_region_2_code
ORDER BY sites_per_1000_people DESC