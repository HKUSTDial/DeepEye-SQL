WITH monthly_data AS (
  SELECT 
    `mth`,
    `product_id`,
    `qty`,
    ROW_NUMBER() OVER (PARTITION BY `product_id` ORDER BY `mth`) AS month_num
  FROM `monthly_sales`
  WHERE `product_id` IN (4160, 7790)
    AND `mth` >= '2016-01-01'
  ORDER BY `mth`
  LIMIT 36
),
cma_calculation AS (
  SELECT 
    m1.`product_id`,
    m1.`mth`,
    m1.month_num,
    m1.`qty`,
    (
      SELECT AVG(m2.`qty`)
      FROM monthly_data m2
      WHERE m2.`product_id` = m1.`product_id`
        AND m2.month_num BETWEEN m1.month_num - 5 AND m1.month_num + 6
    ) AS avg1,
    (
      SELECT AVG(m3.`qty`)
      FROM monthly_data m3
      WHERE m3.`product_id` = m1.`product_id`
        AND m3.month_num BETWEEN m1.month_num - 6 AND m1.month_num + 5
    ) AS avg2
  FROM monthly_data m1
  WHERE m1.month_num BETWEEN 7 AND 30
),
ratios AS (
  SELECT 
    `product_id`,
    `mth`,
    month_num,
    `qty`,
    (avg1 + avg2) / 2.0 AS cma,
    `qty` / ((avg1 + avg2) / 2.0) AS ratio
  FROM cma_calculation
  WHERE avg1 IS NOT NULL AND avg2 IS NOT NULL
),
product_stats AS (
  SELECT 
    `product_id`,
    AVG(ratio) AS avg_ratio,
    AVG(`qty`) AS avg_monthly_sales
  FROM ratios
  GROUP BY `product_id`
),
forecast_annual AS (
  SELECT 
    `product_id`,
    avg_monthly_sales * avg_ratio * 12 AS forecasted_annual_sales
  FROM product_stats
)
SELECT AVG(forecasted_annual_sales) AS avg_forecasted_annual_sales
FROM forecast_annual