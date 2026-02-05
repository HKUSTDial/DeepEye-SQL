WITH avg_birth_rates AS (
  SELECT 
    h.country_code,
    h.country_name,
    c.region,
    AVG(h.value) AS avg_birth_rate
  FROM `bigquery-public-data.world_bank_health_population.health_nutrition_population` h
  INNER JOIN `bigquery-public-data.world_bank_wdi.country_summary` c
    ON h.country_code = c.country_code
  WHERE h.indicator_name = 'Birth rate, crude (per 1,000 people)'
    AND h.year BETWEEN 1980 AND 1989
    AND c.income_group = 'High income'
    AND c.region IS NOT NULL
  GROUP BY h.country_code, h.country_name, c.region
),
ranked AS (
  SELECT 
    region,
    country_name,
    avg_birth_rate,
    RANK() OVER (PARTITION BY region ORDER BY avg_birth_rate DESC) AS rank
  FROM avg_birth_rates
)
SELECT region, country_name, avg_birth_rate
FROM ranked
WHERE rank = 1
ORDER BY region