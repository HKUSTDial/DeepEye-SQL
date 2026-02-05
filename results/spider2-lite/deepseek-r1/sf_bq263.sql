WITH complete_items AS (
  SELECT 
    DATE_TRUNC('month', TO_TIMESTAMP_NTZ(o."created_at" / 1000000)) AS month_date,
    o."order_id" AS order_id,
    oi."sale_price" AS sale_price,
    ii."cost" AS cost
  FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDERS" o
  JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS" oi ON o."order_id" = oi."order_id"
  JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."INVENTORY_ITEMS" ii ON oi."inventory_item_id" = ii."id"
  JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS" p ON oi."product_id" = p."id"
  WHERE o."status" = 'Complete'
    AND TO_TIMESTAMP_NTZ(o."created_at" / 1000000) BETWEEN '2023-01-01' AND '2023-12-31'
    AND p."category" = 'Sleep & Lounge'
)
SELECT 
  TO_CHAR(month_date, 'YYYY-MM') AS month,
  SUM(sale_price) AS total_sales,
  SUM(cost) AS total_cost,
  COUNT(DISTINCT order_id) AS number_of_complete_orders,
  SUM(sale_price) - SUM(cost) AS total_profit,
  CASE WHEN SUM(cost) <> 0 THEN (SUM(sale_price) - SUM(cost)) / SUM(cost) ELSE NULL END AS profit_to_cost_ratio
FROM complete_items
GROUP BY month_date
ORDER BY month_date