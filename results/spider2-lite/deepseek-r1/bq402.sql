WITH visitor_stats AS (
  SELECT 
    fullVisitorId,
    MAX(CASE WHEN totals.transactions IS NOT NULL AND totals.transactions >= 1 THEN 1 ELSE 0 END) AS is_purchaser,
    SUM(totals.transactions) AS total_transactions
  FROM `data-to-insights.ecommerce.web_analytics`
  GROUP BY fullVisitorId
)
SELECT 
  COUNT(DISTINCT fullVisitorId) AS total_visitors,
  SUM(is_purchaser) AS total_purchasers,
  SAFE_DIVIDE(SUM(is_purchaser), COUNT(DISTINCT fullVisitorId)) AS conversion_rate,
  AVG(CASE WHEN is_purchaser = 1 THEN total_transactions ELSE NULL END) AS avg_transactions_per_purchaser
FROM visitor_stats