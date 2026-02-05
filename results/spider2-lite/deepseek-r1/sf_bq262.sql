WITH monthly_sales AS (
  SELECT 
    DATE_TRUNC('month', TO_TIMESTAMP(o."created_at" / 1000000)) AS "month_start",
    p."category" AS "product_category",
    COUNT(DISTINCT oi."order_id") AS "total_orders",
    SUM(oi."sale_price") AS "total_revenue",
    SUM(oi."sale_price" - p."cost") AS "total_profit"
  FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS" oi
  JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDERS" o ON oi."order_id" = o."order_id"
  JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS" p ON oi."product_id" = p."id"
  WHERE TO_TIMESTAMP(o."created_at" / 1000000) >= '2019-06-01'
    AND TO_TIMESTAMP(o."created_at" / 1000000) < '2020-01-01'
  GROUP BY 1, 2
),
with_growth AS (
  SELECT 
    "month_start",
    "product_category",
    "total_orders",
    "total_revenue",
    "total_profit",
    LAG("total_orders") OVER (PARTITION BY "product_category" ORDER BY "month_start") AS "prev_orders",
    LAG("total_revenue") OVER (PARTITION BY "product_category" ORDER BY "month_start") AS "prev_revenue",
    LAG("total_profit") OVER (PARTITION BY "product_category" ORDER BY "month_start") AS "prev_profit"
  FROM monthly_sales
)
SELECT 
  TO_CHAR("month_start", 'YYYY-MM') AS "month",
  "product_category",
  "total_orders",
  "total_revenue",
  "total_profit",
  CASE 
    WHEN "prev_orders" = 0 THEN NULL 
    ELSE (("total_orders" - "prev_orders") * 100.0) / NULLIF("prev_orders", 0) 
  END AS "orders_growth_rate",
  CASE 
    WHEN "prev_revenue" = 0 THEN NULL 
    ELSE (("total_revenue" - "prev_revenue") * 100.0) / NULLIF("prev_revenue", 0) 
  END AS "revenue_growth_rate",
  CASE 
    WHEN "prev_profit" = 0 THEN NULL 
    ELSE (("total_profit" - "prev_profit") * 100.0) / NULLIF("prev_profit", 0) 
  END AS "profit_growth_rate"
FROM with_growth
WHERE "month_start" > '2019-06-01'
ORDER BY "month_start", "product_category"