WITH monthly_metrics AS (
  SELECT
    DATE_TRUNC('month', TO_TIMESTAMP(o."created_at" / 1000000)) AS month_start,
    TO_CHAR(DATE_TRUNC('month', TO_TIMESTAMP(o."created_at" / 1000000)), 'YYYY-MM') AS month_formatted,
    p."category" AS product_category,
    COUNT(DISTINCT oi."order_id") AS total_orders,
    SUM(oi."sale_price") AS total_revenue,
    SUM(oi."sale_price" - p."cost") AS total_profit
  FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS" oi
  JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDERS" o ON oi."order_id" = o."order_id"
  JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS" p ON oi."product_id" = p."id"
  WHERE TO_TIMESTAMP(o."created_at" / 1000000) >= '2019-06-01'
    AND TO_TIMESTAMP(o."created_at" / 1000000) < '2020-01-01'
  GROUP BY 1, 2, 3
),
with_growth AS (
  SELECT
    month_formatted,
    product_category,
    total_orders,
    total_revenue,
    total_profit,
    LAG(total_orders, 1) OVER (PARTITION BY product_category ORDER BY month_start) AS prev_month_orders,
    LAG(total_revenue, 1) OVER (PARTITION BY product_category ORDER BY month_start) AS prev_month_revenue,
    LAG(total_profit, 1) OVER (PARTITION BY product_category ORDER BY month_start) AS prev_month_profit
  FROM monthly_metrics
)
SELECT
  month_formatted AS month,
  product_category,
  total_orders,
  total_revenue,
  total_profit,
  CASE 
    WHEN prev_month_orders IS NOT NULL AND prev_month_orders != 0 
    THEN (total_orders - prev_month_orders) * 100.0 / prev_month_orders 
    ELSE NULL 
  END AS orders_growth_rate,
  CASE 
    WHEN prev_month_revenue IS NOT NULL AND prev_month_revenue != 0 
    THEN (total_revenue - prev_month_revenue) * 100.0 / prev_month_revenue 
    ELSE NULL 
  END AS revenue_growth_rate,
  CASE 
    WHEN prev_month_profit IS NOT NULL AND prev_month_profit != 0 
    THEN (total_profit - prev_month_profit) * 100.0 / prev_month_profit 
    ELSE NULL 
  END AS profit_growth_rate
FROM with_growth
WHERE month_formatted != '2019-06'
ORDER BY month_formatted, product_category