WITH combined_data AS (
  SELECT 
    state_name,
    'postal_code' AS level,
    count_qualified,
    percent_covered,
    percent_qualified,
    number_of_panels_total,
    kw_total,
    yearly_sunlight_kwh_total,
    carbon_offset_metric_tons,
    existing_installs_count
  FROM `bigquery-public-data.sunroof_solar.solar_potential_by_postal_code`
  
  UNION ALL
  
  SELECT 
    state_name,
    'census_tract' AS level,
    count_qualified,
    percent_covered,
    percent_qualified,
    number_of_panels_total,
    kw_total,
    yearly_sunlight_kwh_total,
    carbon_offset_metric_tons,
    existing_installs_count
  FROM `bigquery-public-data.sunroof_solar.solar_potential_by_censustract`
)
SELECT 
  state_name,
  level,
  SUM(count_qualified) AS total_buildings_available,
  AVG(percent_covered) AS avg_percent_covered,
  AVG(percent_qualified) AS avg_percent_suitable,
  SUM(number_of_panels_total) AS total_potential_panels,
  SUM(kw_total) AS total_kilowatt_capacity,
  SUM(yearly_sunlight_kwh_total) AS total_energy_generation_potential,
  SUM(carbon_offset_metric_tons) AS total_carbon_offset,
  SUM(existing_installs_count) AS current_installations,
  SUM(count_qualified * (percent_covered/100) * (percent_qualified/100)) - SUM(existing_installs_count) AS gap_potential_installations
FROM combined_data
GROUP BY state_name, level
ORDER BY state_name, level