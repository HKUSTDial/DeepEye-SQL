SELECT indicator_name, MAX(value) AS highest_debt_value
FROM `bigquery-public-data.world_bank_intl_debt.international_debt`
WHERE country_code = 'RUS' AND value IS NOT NULL AND NOT IS_NAN(value)
GROUP BY indicator_name
ORDER BY highest_debt_value DESC
LIMIT 3