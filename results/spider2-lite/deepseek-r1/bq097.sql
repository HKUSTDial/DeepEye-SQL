SELECT 
  GeoName,
  GeoFIPS,
  MAX(CASE WHEN EXTRACT(YEAR FROM Year) = 2017 THEN Earnings_per_job_avg END) - MAX(CASE WHEN EXTRACT(YEAR FROM Year) = 2012 THEN Earnings_per_job_avg END) AS increase_amount
FROM `bigquery-public-data.sdoh_bea_cainc30.fips`
WHERE GeoName LIKE '%, MA'
  AND EXTRACT(YEAR FROM Year) IN (2012, 2017)
GROUP BY GeoName, GeoFIPS
HAVING COUNT(*) = 2
ORDER BY increase_amount DESC