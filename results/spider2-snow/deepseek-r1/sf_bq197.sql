WITH complete_orders AS (
  SELECT 
    "order_id",
    "created_at"
  FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDERS"
  WHERE "status" = 'Complete'
    AND TO_TIMESTAMP("created_at" / 1000000) < '2024-07-01'
),
order_details AS (
  SELECT 
    o."order_id",
    o."created_at" AS "order_created_at",
    oi."id" AS "order_item_id",
    oi."product_id",
    oi."sale_price"
  FROM complete_orders o
  JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS" oi 
    ON o."order_id" = oi."order_id"
  WHERE oi."status" = 'Complete'
),
product_info AS (
  SELECT 
    od."order_created_at",
    od."order_item_id",
    od."product_id",
    od."sale_price",
    p."name" AS "product_name",
    p."brand",
    p."category"
  FROM order_details od
  JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS" p 
    ON od."product_id" = p."id"
  WHERE p."brand" IS NOT NULL
),
aggregated AS (
  SELECT 
    DATE_TRUNC('month', TO_TIMESTAMP("order_created_at" / 1000000)) AS "month_start",
    "product_id",
    "product_name",
    "brand",
    "category",
    COUNT("order_item_id") AS "total_sales",
    SUM("sale_price") AS "total_revenue"
  FROM product_info
  GROUP BY "month_start", "product_id", "product_name", "brand", "category"
),
ranked AS (
  SELECT 
    "month_start",
    "product_id",
    "product_name",
    "brand",
    "category",
    "total_sales",
    "total_revenue",
    ROW_NUMBER() OVER (PARTITION BY "month_start" ORDER BY "total_sales" DESC, "total_revenue" DESC) AS "rn"
  FROM aggregated
)
SELECT 
  TO_CHAR("month_start", 'YYYY-MM') AS "month",
  "product_name",
  "brand",
  "category",
  "total_sales",
  ROUND("total_revenue", 2) AS "rounded_total_revenue",
  'Complete' AS "order_status"
FROM ranked
WHERE "rn" = 1
ORDER BY "month_start"