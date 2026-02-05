WITH population_increase AS (
  SELECT `country_code`
  FROM `bigquery-public-data.world_bank_global_population.population_by_country`
  WHERE `year_2018` IS NOT NULL AND `year_2017` IS NOT NULL
    AND `year_2017` > 0
    AND (`year_2018` - `year_2017`) / `year_2017` > 0.01
),
target_indicator AS (
  SELECT `series_code`
  FROM `bigquery-public-data.world_bank_health_population.series_summary`
  WHERE LOWER(`indicator_name`) LIKE '%current health expenditure%'
    AND LOWER(`indicator_name`) LIKE '%per capita%'
    AND LOWER(`indicator_name`) LIKE '%ppp%'
),
health_data AS (
  SELECT 
    h.`country_code`,
    MAX(CASE WHEN h.`year` = 2017 THEN h.`value` END) AS health_2017,
    MAX(CASE WHEN h.`year` = 2018 THEN h.`value` END) AS health_2018
  FROM `bigquery-public-data.world_bank_health_population.health_nutrition_population` h
  INNER JOIN target_indicator t ON h.`indicator_code` = t.`series_code`
  WHERE h.`year` IN (2017, 2018)
  GROUP BY h.`country_code`
  HAVING health_2017 IS NOT NULL AND health_2018 IS NOT NULL
    AND health_2017 > 0
    AND (health_2018 - health_2017) / health_2017 > 0.01
)
SELECT COUNT(DISTINCT p.`country_code`) AS count_countries
FROM population_increase p
INNER JOIN health_data h ON p.`country_code` = h.`country_code`