WITH "completed_orders_with_category" AS (
  SELECT DISTINCT
    "o"."order_id" AS "order_id",
    EXTRACT(YEAR FROM TO_TIMESTAMP("o"."created_at" / 1000000)) AS "year",
    EXTRACT(MONTH FROM TO_TIMESTAMP("o"."created_at" / 1000000)) AS "month",
    "p"."category" AS "category"
  FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDERS" "o"
  JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS" "oi"
    ON "o"."order_id" = "oi"."order_id"
  JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS" "p"
    ON "oi"."product_id" = "p"."id"
  WHERE "o"."status" = 'Complete'
),
"monthly_order_counts" AS (
  SELECT
    "category",
    "year",
    "month",
    COUNT(*) AS "order_count"
  FROM "completed_orders_with_category"
  GROUP BY "category", "year", "month"
),
"order_growth_rates" AS (
  SELECT
    "category",
    "year",
    "month",
    "order_count",
    LAG("order_count") OVER (PARTITION BY "category" ORDER BY "year", "month") AS "prev_order_count",
    ("order_count" - LAG("order_count") OVER (PARTITION BY "category" ORDER BY "year", "month")) * 100.0 / NULLIF(LAG("order_count") OVER (PARTITION BY "category" ORDER BY "year", "month"), 0) AS "growth_rate"
  FROM "monthly_order_counts"
),
"avg_order_growth" AS (
  SELECT
    "category",
    AVG("growth_rate") AS "avg_order_growth_rate"
  FROM "order_growth_rates"
  WHERE "growth_rate" IS NOT NULL
  GROUP BY "category"
),
"top_category" AS (
  SELECT "category"
  FROM "avg_order_growth"
  ORDER BY "avg_order_growth_rate" DESC
  LIMIT 1
),
"monthly_revenue" AS (
  SELECT
    EXTRACT(YEAR FROM TO_TIMESTAMP("o"."created_at" / 1000000)) AS "year",
    EXTRACT(MONTH FROM TO_TIMESTAMP("o"."created_at" / 1000000)) AS "month",
    SUM("oi"."sale_price") AS "total_revenue"
  FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDERS" "o"
  JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS" "oi"
    ON "o"."order_id" = "oi"."order_id"
  JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS" "p"
    ON "oi"."product_id" = "p"."id"
  WHERE "o"."status" = 'Complete'
    AND "p"."category" = (SELECT "category" FROM "top_category")
  GROUP BY "year", "month"
),
"revenue_growth_rates" AS (
  SELECT
    "year",
    "month",
    "total_revenue",
    LAG("total_revenue") OVER (ORDER BY "year", "month") AS "prev_revenue",
    ("total_revenue" - LAG("total_revenue") OVER (ORDER BY "year", "month")) * 100.0 / NULLIF(LAG("total_revenue") OVER (ORDER BY "year", "month"), 0) AS "revenue_growth_rate"
  FROM "monthly_revenue"
)
SELECT
  (SELECT "category" FROM "top_category") AS "top_category",
  AVG("revenue_growth_rate") AS "avg_revenue_growth_rate"
FROM "revenue_growth_rates"
WHERE "revenue_growth_rate" IS NOT NULL