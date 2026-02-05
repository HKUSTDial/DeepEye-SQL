WITH purchase_events AS (
  SELECT 
    ecommerce.transaction_id AS transaction_id,
    ecommerce.total_item_quantity AS total_item_quantity,
    ecommerce.purchase_revenue_in_usd AS purchase_revenue_in_usd,
    ecommerce.purchase_revenue AS purchase_revenue,
    ecommerce.tax_value_in_usd AS tax_value_in_usd,
    items.item_category AS item_category
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20201130`,
  UNNEST(items) AS items
  WHERE event_name = 'purchase'
    AND ecommerce.purchase_revenue_in_usd > 0
),
category_tax_rates AS (
  SELECT 
    item_category,
    SUM(tax_value_in_usd) / SUM(purchase_revenue_in_usd) AS tax_rate
  FROM purchase_events
  WHERE item_category IS NOT NULL
  GROUP BY item_category
),
top_category AS (
  SELECT item_category
  FROM category_tax_rates
  ORDER BY tax_rate DESC
  LIMIT 1
)
SELECT DISTINCT
  transaction_id,
  total_item_quantity,
  purchase_revenue_in_usd,
  purchase_revenue
FROM purchase_events
WHERE item_category = (SELECT item_category FROM top_category)
ORDER BY transaction_id