WITH yearly_revenue AS (
  SELECT 
    `trafficSource`.`source` AS traffic_source,
    SUM(`totals`.`totalTransactionRevenue`) AS total_revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20171231'
    AND `totals`.`totalTransactionRevenue` IS NOT NULL
  GROUP BY `trafficSource`.`source`
  ORDER BY total_revenue DESC
  LIMIT 1
),
monthly_revenue AS (
  SELECT 
    EXTRACT(MONTH FROM PARSE_DATE('%Y%m%d', `date`)) AS month,
    SUM(`totals`.`totalTransactionRevenue`) AS monthly_revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20171231'
    AND `trafficSource`.`source` = (SELECT traffic_source FROM yearly_revenue)
    AND `totals`.`totalTransactionRevenue` IS NOT NULL
  GROUP BY month
)
SELECT 
  (SELECT traffic_source FROM yearly_revenue) AS traffic_source,
  ROUND((MAX(mr.monthly_revenue) - MIN(mr.monthly_revenue)) / 1000000.0, 2) AS revenue_difference_millions
FROM monthly_revenue AS mr;