WITH top_drg AS (
  SELECT drg_definition
  FROM `bigquery-public-data.cms_medicare.inpatient_charges_2014`
  GROUP BY drg_definition
  ORDER BY SUM(total_discharges) DESC
  LIMIT 1
), city_ranking AS (
  SELECT 
    provider_city,
    SUM(total_discharges) AS city_discharges,
    SUM(average_total_payments * total_discharges) / SUM(total_discharges) AS weighted_avg_payments
  FROM `bigquery-public-data.cms_medicare.inpatient_charges_2014`
  WHERE drg_definition = (SELECT drg_definition FROM top_drg)
  GROUP BY provider_city
  ORDER BY city_discharges DESC
  LIMIT 3
)
SELECT 
  (SELECT drg_definition FROM top_drg) AS drg_definition,
  provider_city,
  weighted_avg_payments
FROM city_ranking
ORDER BY city_discharges DESC