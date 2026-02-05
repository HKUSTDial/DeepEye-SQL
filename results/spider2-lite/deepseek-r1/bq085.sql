WITH country_mapping AS (
  SELECT 'United States' AS desired_name, 'US' AS covid_name, 'United States' AS wb_name UNION ALL
  SELECT 'France', 'France', 'France' UNION ALL
  SELECT 'China', 'China', 'China' UNION ALL
  SELECT 'Italy', 'Italy', 'Italy' UNION ALL
  SELECT 'Spain', 'Spain', 'Spain' UNION ALL
  SELECT 'Germany', 'Germany', 'Germany' UNION ALL
  SELECT 'Iran', 'Iran', 'Iran'
),
covid_cases AS (
  SELECT country_region, confirmed
  FROM `bigquery-public-data.covid19_jhu_csse.summary`
  WHERE date = DATE '2020-04-20'
    AND country_region IN (SELECT covid_name FROM country_mapping)
),
population_2020 AS (
  SELECT country_name, SUM(value) AS population
  FROM `bigquery-public-data.world_bank_wdi.indicators_data`
  WHERE year = 2020
    AND indicator_code = 'SP.POP.TOTL'
    AND country_name IN (SELECT wb_name FROM country_mapping)
  GROUP BY country_name
)
SELECT 
  cm.desired_name AS country,
  cc.confirmed AS total_confirmed_cases,
  ROUND(SAFE_DIVIDE(cc.confirmed, p.population) * 100000, 2) AS cases_per_100k
FROM country_mapping cm
LEFT JOIN covid_cases cc ON cm.covid_name = cc.country_region
LEFT JOIN population_2020 p ON cm.wb_name = p.country_name
ORDER BY cm.desired_name