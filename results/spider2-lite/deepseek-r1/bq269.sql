WITH sessions AS (
  SELECT 
    fullVisitorId,
    totals.pageviews AS pageviews,
    IF(totals.transactions > 0, 'purchase', 'non_purchase') AS classification,
    EXTRACT(MONTH FROM PARSE_DATE('%Y%m%d', date)) AS month
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE _TABLE_SUFFIX BETWEEN '20170601' AND '20170731'
    AND totals.pageviews IS NOT NULL
),
visitor_month_sums AS (
  SELECT 
    fullVisitorId,
    month,
    SUM(CASE WHEN classification = 'purchase' THEN pageviews ELSE 0 END) AS purchase_pageviews,
    SUM(CASE WHEN classification = 'non_purchase' THEN pageviews ELSE 0 END) AS non_purchase_pageviews
  FROM sessions
  GROUP BY fullVisitorId, month
)
SELECT 
  month,
  AVG(CASE WHEN purchase_pageviews > 0 THEN purchase_pageviews END) AS avg_pageviews_purchase,
  AVG(CASE WHEN non_purchase_pageviews > 0 THEN non_purchase_pageviews END) AS avg_pageviews_non_purchase
FROM visitor_month_sums
GROUP BY month
ORDER BY month;