SELECT 
  d.country_name,
  SUM(d.value) AS total_debt
FROM `bigquery-public-data.world_bank_intl_debt.international_debt` d
INNER JOIN `bigquery-public-data.world_bank_intl_debt.country_summary` c ON d.country_code = c.country_code
INNER JOIN `bigquery-public-data.world_bank_intl_debt.series_summary` s ON d.indicator_code = s.series_code
WHERE (c.region IS NOT NULL AND c.region != 'NULL')
  AND NOT IS_NAN(d.value)
  AND REGEXP_CONTAINS(s.indicator_name, r'(?i)long-term.*external debt|external debt.*long-term')
GROUP BY d.country_name
ORDER BY total_debt DESC
LIMIT 10