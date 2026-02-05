WITH product_revenue_data AS (
  SELECT
    `trafficSource`.`source` AS source,
    PARSE_DATE('%Y%m%d', `date`) AS session_date,
    `product`.`productRevenue` AS productRevenue
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
    UNNEST(`hits`) AS hits,
    UNNEST(hits.`product`) AS product
  WHERE
    _TABLE_SUFFIX BETWEEN '20170101' AND '20170630'
    AND `product`.`productRevenue` IS NOT NULL
),
total_per_source AS (
  SELECT
    source,
    SUM(productRevenue) AS total_revenue
  FROM product_revenue_data
  GROUP BY source
  ORDER BY total_revenue DESC
  LIMIT 1
),
daily_totals AS (
  SELECT
    source,
    session_date,
    SUM(productRevenue) AS daily_revenue
  FROM product_revenue_data
  WHERE source = (SELECT source FROM total_per_source)
  GROUP BY source, session_date
),
weekly_totals AS (
  SELECT
    source,
    DATE_TRUNC(session_date, WEEK(MONDAY)) AS week_start,
    SUM(productRevenue) AS weekly_revenue
  FROM product_revenue_data
  WHERE source = (SELECT source FROM total_per_source)
  GROUP BY source, week_start
),
monthly_totals AS (
  SELECT
    source,
    DATE_TRUNC(session_date, MONTH) AS month_start,
    SUM(productRevenue) AS monthly_revenue
  FROM product_revenue_data
  WHERE source = (SELECT source FROM total_per_source)
  GROUP BY source, month_start
)
SELECT
  (SELECT source FROM total_per_source) AS top_traffic_source,
  MAX(daily_revenue) / 1000000000000 AS max_daily_revenue_millions,
  MAX(weekly_revenue) / 1000000000000 AS max_weekly_revenue_millions,
  MAX(monthly_revenue) / 1000000000000 AS max_monthly_revenue_millions
FROM daily_totals
CROSS JOIN weekly_totals
CROSS JOIN monthly_totals