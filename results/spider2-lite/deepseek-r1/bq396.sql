WITH weather_counts AS (
  SELECT
    state_name,
    COUNTIF(atmospheric_conditions_1 = 2) AS rainy_count,
    COUNTIF(atmospheric_conditions_1 = 1) AS clear_count
  FROM `bigquery-public-data.nhtsa_traffic_fatalities.accident_2016`
  WHERE day_of_week IN (1, 7)
    AND atmospheric_conditions_1 IN (1, 2)
  GROUP BY state_name
)
SELECT
  state_name,
  ABS(rainy_count - clear_count) AS difference
FROM weather_counts
WHERE rainy_count > 0 AND clear_count > 0
ORDER BY difference DESC
LIMIT 3