WITH inpatient_total AS (
  SELECT 
    provider_id,
    SUM(average_medicare_payments * total_discharges) AS total_cost
  FROM `bigquery-public-data.cms_medicare.inpatient_charges_*`
  WHERE _TABLE_SUFFIX BETWEEN '2011' AND '2015'
  GROUP BY provider_id
),
top_provider AS (
  SELECT provider_id
  FROM inpatient_total
  ORDER BY total_cost DESC
  LIMIT 1
),
inpatient_yearly AS (
  SELECT 
    CAST(_TABLE_SUFFIX AS INT64) AS year,
    AVG(average_medicare_payments * total_discharges) AS avg_inpatient_cost
  FROM `bigquery-public-data.cms_medicare.inpatient_charges_*`
  WHERE _TABLE_SUFFIX BETWEEN '2011' AND '2015'
    AND provider_id = (SELECT provider_id FROM top_provider)
  GROUP BY year
),
outpatient_yearly AS (
  SELECT 
    CAST(_TABLE_SUFFIX AS INT64) AS year,
    AVG(average_total_payments * outpatient_services) AS avg_outpatient_cost
  FROM `bigquery-public-data.cms_medicare.outpatient_charges_*`
  WHERE _TABLE_SUFFIX BETWEEN '2011' AND '2015'
    AND provider_id = (SELECT provider_id FROM top_provider)
  GROUP BY year
),
years AS (
  SELECT year FROM UNNEST([2011, 2012, 2013, 2014, 2015]) AS year
)
SELECT 
  years.year,
  inpatient_yearly.avg_inpatient_cost,
  outpatient_yearly.avg_outpatient_cost
FROM years
LEFT JOIN inpatient_yearly ON years.year = inpatient_yearly.year
LEFT JOIN outpatient_yearly ON years.year = outpatient_yearly.year
ORDER BY years.year