WITH monthly_metrics AS (
  SELECT 
    p."category",
    EXTRACT(YEAR FROM TO_TIMESTAMP(o."delivered_at" / 1000000)) AS "year",
    EXTRACT(MONTH FROM TO_TIMESTAMP(o."delivered_at" / 1000000)) AS "month",
    SUM(oi."sale_price") AS "total_revenue",
    COUNT(DISTINCT o."order_id") AS "total_orders",
    SUM(p."cost") AS "total_cost"
  FROM 
    "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS" oi
    JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS" p ON oi."product_id" = p."id"
    JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDERS" o ON oi."order_id" = o."order_id"
  WHERE 
    o."status" = 'Complete'
    AND o."delivered_at" IS NOT NULL
    AND TO_TIMESTAMP(o."delivered_at" / 1000000) < '2022-01-01'
  GROUP BY 
    p."category",
    EXTRACT(YEAR FROM TO_TIMESTAMP(o."delivered_at" / 1000000)),
    EXTRACT(MONTH FROM TO_TIMESTAMP(o."delivered_at" / 1000000))
),
with_previous_month AS (
  SELECT 
    "category",
    "year",
    "month",
    "total_revenue",
    "total_orders",
    "total_cost",
    "total_revenue" - "total_cost" AS "total_profit",
    ("total_revenue" - "total_cost") / NULLIF("total_cost", 0) AS "profit_to_cost_ratio",
    LAG("total_revenue") OVER (PARTITION BY "category" ORDER BY "year", "month") AS "prev_month_revenue",
    LAG("total_orders") OVER (PARTITION BY "category" ORDER BY "year", "month") AS "prev_month_orders"
  FROM 
    monthly_metrics
)
SELECT 
  "category",
  "year",
  "month",
  "total_revenue",
  "total_orders",
  "total_cost",
  "total_profit",
  "profit_to_cost_ratio",
  CASE 
    WHEN "prev_month_revenue" IS NULL OR "prev_month_revenue" = 0 THEN NULL
    ELSE (("total_revenue" - "prev_month_revenue") / "prev_month_revenue") * 100
  END AS "revenue_growth_percentage",
  CASE 
    WHEN "prev_month_orders" IS NULL OR "prev_month_orders" = 0 THEN NULL
    ELSE (("total_orders" - "prev_month_orders") / "prev_month_orders") * 100
  END AS "orders_growth_percentage"
FROM 
  with_previous_month
ORDER BY 
  "category",
  "year",
  "month"