WITH youtube_buyers AS (
  SELECT DISTINCT `fullVisitorId`
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
  WHERE _TABLE_SUFFIX BETWEEN '01' AND '31'
  AND EXISTS (
    SELECT 1
    FROM UNNEST(hits) h, UNNEST(h.product) p
    WHERE h.eCommerceAction.action_type = '6'
    AND p.v2ProductName LIKE '%YouTube%'
  )
)
SELECT 
  p.v2ProductName as product_name,
  SUM(p.productQuantity) as total_quantity_purchased
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` sessions
CROSS JOIN UNNEST(hits) h
CROSS JOIN UNNEST(h.product) p
INNER JOIN youtube_buyers yb ON sessions.fullVisitorId = yb.fullVisitorId
WHERE _TABLE_SUFFIX BETWEEN '01' AND '31'
AND h.eCommerceAction.action_type = '6'
AND p.v2ProductName NOT LIKE '%YouTube%'
GROUP BY product_name
ORDER BY total_quantity_purchased DESC
LIMIT 1