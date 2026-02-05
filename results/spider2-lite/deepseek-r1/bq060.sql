SELECT b.country_name, b.net_migration
FROM `bigquery-public-data.census_bureau_international.country_names_area` AS a
JOIN `bigquery-public-data.census_bureau_international.birth_death_growth_rates` AS b
ON a.country_code = b.country_code
WHERE b.year = 2017 AND a.country_area > 500
ORDER BY b.net_migration DESC
LIMIT 3