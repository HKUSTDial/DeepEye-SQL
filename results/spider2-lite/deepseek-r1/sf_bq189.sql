WITH monthly_order_counts AS (
  SELECT 
    p."category" AS "category",
    DATE_TRUNC('month', TO_TIMESTAMP(o."created_at" / 1000000)) AS "month",
    COUNT(DISTINCT o."order_id") AS "order_count"
  FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDERS" o
  INNER JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS" oi 
    ON o."order_id" = oi."order_id"
  INNER JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS" p 
    ON oi."product_id" = p."id"
  WHERE o."status" = 'Complete'
  GROUP BY p."category", "month"
),
order_growth_rates AS (
  SELECT 
    "category",
    "month",
    "order_count",
    LAG("order_count") OVER (PARTITION BY "category" ORDER BY "month") AS "prev_order_count",
    ("order_count" - LAG("order_count") OVER (PARTITION BY "category" ORDER BY "month")) 
      / NULLIF(LAG("order_count") OVER (PARTITION BY "category" ORDER BY "month"), 0) * 100 AS "growth_rate"
  FROM monthly_order_counts
),
avg_order_growth AS (
  SELECT 
    "category",
    AVG("growth_rate") AS "avg_order_growth_rate"
  FROM order_growth_rates
  WHERE "prev_order_count" IS NOT NULL AND "prev_order_count" != 0
  GROUP BY "category"
),
top_category AS (
  SELECT "category"
  FROM avg_order_growth
  ORDER BY "avg_order_growth_rate" DESC
  LIMIT 1
),
monthly_revenue AS (
  SELECT 
    DATE_TRUNC('month', TO_TIMESTAMP(o."created_at" / 1000000)) AS "month",
    SUM(oi."sale_price") AS "revenue"
  FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDERS" o
  INNER JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS" oi 
    ON o."order_id" = oi."order_id"
  INNER JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS" p 
    ON oi."product_id" = p."id"
  WHERE o."status" = 'Complete'
    AND p."category" = (SELECT "category" FROM top_category)
  GROUP BY "month"
),
revenue_growth_rates AS (
  SELECT 
    "month",
    "revenue",
    LAG("revenue") OVER (ORDER BY "month") AS "prev_revenue",
    ("revenue" - LAG("revenue") OVER (ORDER BY "month")) 
      / NULLIF(LAG("revenue") OVER (ORDER BY "month"), 0) * 100 AS "growth_rate"
  FROM monthly_revenue
)
SELECT 
  (SELECT "category" FROM top_category) AS "top_category",
  AVG("growth_rate") AS "avg_monthly_revenue_growth_rate"
FROM revenue_growth_rates
WHERE "prev_revenue" IS NOT NULL AND "prev_revenue" != 0;