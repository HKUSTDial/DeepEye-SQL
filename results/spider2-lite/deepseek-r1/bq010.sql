WITH customers_who_bought_target_product AS (
  SELECT DISTINCT 
    `fullVisitorId`
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE _TABLE_SUFFIX BETWEEN '20170701' AND '20170731'
    AND EXISTS (
      SELECT 1 
      FROM UNNEST(hits) AS hit, UNNEST(hit.product) AS prod
      WHERE prod.v2ProductName = 'Youtube Men\'s Vintage Henley'
        AND hit.eCommerceAction.action_type = '6'
    )
),
all_purchases_by_these_customers AS (
  SELECT 
    prod.v2ProductName AS product_name,
    SUM(prod.productQuantity) AS total_quantity
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` AS sessions
  JOIN customers_who_bought_target_product AS target_customers
    ON sessions.`fullVisitorId` = target_customers.`fullVisitorId`
  CROSS JOIN UNNEST(sessions.hits) AS hit
  CROSS JOIN UNNEST(hit.product) AS prod
  WHERE _TABLE_SUFFIX BETWEEN '20170701' AND '20170731'
    AND hit.eCommerceAction.action_type = '6'
    AND prod.v2ProductName != 'Youtube Men\'s Vintage Henley'
    AND prod.productQuantity IS NOT NULL
  GROUP BY prod.v2ProductName
)
SELECT 
  product_name,
  total_quantity
FROM all_purchases_by_these_customers
ORDER BY total_quantity DESC
LIMIT 1