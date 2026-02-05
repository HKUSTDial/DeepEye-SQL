WITH classified_sessions AS (
  SELECT
    PARSE_DATE('%Y%m%d', `date`) AS session_date,
    EXTRACT(MONTH FROM PARSE_DATE('%Y%m%d', `date`)) AS month,
    `fullVisitorId`,
    COALESCE(`totals`.`pageviews`, 0) AS pageviews,
    CASE 
      WHEN `totals`.`transactions` >= 1 AND EXISTS (SELECT 1 FROM UNNEST(`hits`) h, UNNEST(h.product) p WHERE p.productRevenue IS NOT NULL) THEN 'purchase'
      WHEN `totals`.`transactions` IS NULL AND NOT EXISTS (SELECT 1 FROM UNNEST(`hits`) h, UNNEST(h.product) p WHERE p.productRevenue IS NOT NULL) THEN 'non-purchase'
      ELSE 'other'
    END AS purchase_flag
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE _TABLE_SUFFIX BETWEEN '20170401' AND '20170731'
),
per_visitor_monthly AS (
  SELECT
    month,
    purchase_flag,
    fullVisitorId,
    SUM(pageviews) AS total_pageviews_per_visitor
  FROM classified_sessions
  WHERE purchase_flag IN ('purchase', 'non-purchase')
  GROUP BY month, purchase_flag, fullVisitorId
)
SELECT
  month,
  purchase_flag,
  AVG(total_pageviews_per_visitor) AS avg_pageviews_per_visitor
FROM per_visitor_monthly
GROUP BY month, purchase_flag
ORDER BY month, purchase_flag