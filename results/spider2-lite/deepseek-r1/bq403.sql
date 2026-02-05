WITH combined AS (
  SELECT 
    CAST(_TABLE_SUFFIX AS INT64) AS year,
    totrevenue AS revenue,
    totfuncexpns AS expenses
  FROM `bigquery-public-data.irs_990.irs_990_*`
  WHERE _TABLE_SUFFIX BETWEEN '2012' AND '2017'
    AND totrevenue IS NOT NULL 
    AND totfuncexpns IS NOT NULL
  UNION ALL
  SELECT 
    CAST(_TABLE_SUFFIX AS INT64) AS year,
    totrevnue AS revenue,
    totexpns AS expenses
  FROM `bigquery-public-data.irs_990.irs_990_ez_*`
  WHERE _TABLE_SUFFIX BETWEEN '2012' AND '2017'
    AND totrevnue IS NOT NULL 
    AND totexpns IS NOT NULL
  UNION ALL
  SELECT 
    CAST(_TABLE_SUFFIX AS INT64) AS year,
    totrcptperbks AS revenue,
    totexpnsexempt AS expenses
  FROM `bigquery-public-data.irs_990.irs_990_pf_*`
  WHERE _TABLE_SUFFIX BETWEEN '2012' AND '2017'
    AND totrcptperbks IS NOT NULL 
    AND totexpnsexempt IS NOT NULL
)
SELECT 
  year,
  ABS(APPROX_QUANTILES(revenue, 2)[OFFSET(1)] - APPROX_QUANTILES(expenses, 2)[OFFSET(1)]) AS difference
FROM combined
GROUP BY year
ORDER BY difference ASC
LIMIT 3