SELECT 
  t1.country_name,
  t1.under20_pop AS total_population_under_20,
  t2.midyear_population AS total_midyear_population,
  SAFE_DIVIDE(t1.under20_pop, t2.midyear_population) * 100 AS percentage_under_20
FROM (
  SELECT 
    country_code,
    country_name,
    SUM(population) AS under20_pop
  FROM `bigquery-public-data.census_bureau_international.midyear_population_agespecific`
  WHERE year = 2020 AND age < 20
  GROUP BY country_code, country_name
) AS t1
JOIN (
  SELECT 
    country_code,
    country_name,
    midyear_population
  FROM `bigquery-public-data.census_bureau_international.midyear_population`
  WHERE year = 2020
) AS t2
ON t1.country_code = t2.country_code
ORDER BY percentage_under_20 DESC
LIMIT 10