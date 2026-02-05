SELECT 
  covid_data.country_code,
  pop_data.country AS country_name,
  covid_data.cumulative_confirmed,
  pop_data.year_2018 AS population_2018,
  (covid_data.cumulative_confirmed * 100.0 / pop_data.year_2018) AS confirmed_percentage
FROM (
  SELECT 
    iso_3166_1_alpha_3 AS country_code,
    MAX(cumulative_confirmed) AS cumulative_confirmed
  FROM `bigquery-public-data.covid19_open_data.covid19_open_data`
  WHERE date <= '2020-06-30' 
    AND aggregation_level = 0
    AND cumulative_confirmed IS NOT NULL
  GROUP BY iso_3166_1_alpha_3
) AS covid_data
JOIN (
  SELECT 
    country_code,
    country,
    year_2018
  FROM `bigquery-public-data.world_bank_global_population.population_by_country`
  WHERE year_2018 IS NOT NULL 
    AND year_2018 > 0
) AS pop_data
ON covid_data.country_code = pop_data.country_code
ORDER BY confirmed_percentage DESC