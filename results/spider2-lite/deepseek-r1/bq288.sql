WITH state_assets AS (
  SELECT state, SUM(total_assets) AS total_assets_sum
  FROM `bigquery-public-data.fdic_banks.institutions`
  WHERE established_date BETWEEN DATE('1900-01-01') AND DATE('2000-12-31')
    AND institution_name LIKE 'Bank%'
  GROUP BY state
),
top_state AS (
  SELECT state
  FROM state_assets
  ORDER BY total_assets_sum DESC
  LIMIT 1
)
SELECT COUNT(*) AS total_institutions
FROM `bigquery-public-data.fdic_banks.institutions`
WHERE state = (SELECT state FROM top_state)