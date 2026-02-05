WITH us_daily AS (
  SELECT 
    date,
    new_confirmed,
    cumulative_confirmed
  FROM `bigquery-public-data.covid19_open_data.covid19_open_data`
  WHERE country_code = 'US'
    AND aggregation_level = 1
    AND date IS NOT NULL
),
with_prev AS (
  SELECT 
    date,
    new_confirmed,
    cumulative_confirmed,
    LAG(cumulative_confirmed) OVER (ORDER BY date) AS prev_cumulative
  FROM us_daily
),
growth_rates AS (
  SELECT 
    date,
    SAFE_DIVIDE(new_confirmed, prev_cumulative) AS growth_rate
  FROM with_prev
  WHERE prev_cumulative IS NOT NULL
    AND prev_cumulative > 0
    AND EXTRACT(MONTH FROM date) IN (3,4)
),
max_growth AS (
  SELECT 
    date,
    growth_rate
  FROM growth_rates
  ORDER BY growth_rate DESC
  LIMIT 1
)
SELECT 
  FORMAT_DATE('%m-%d', date) AS day_mm_dd
FROM max_growth