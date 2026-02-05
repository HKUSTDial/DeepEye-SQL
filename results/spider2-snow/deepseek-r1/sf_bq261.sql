WITH "order_item_profits" AS (
  SELECT 
    "oi"."product_id" AS "product_id",
    "p"."name" AS "product_name",
    "p"."cost" AS "product_cost",
    "oi"."sale_price" AS "sale_price",
    DATE_TRUNC('month', TO_TIMESTAMP("oi"."created_at" / 1000000)) AS "month"
  FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS" AS "oi"
  JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS" AS "p" ON "oi"."product_id" = "p"."id"
),
"monthly_product_profits" AS (
  SELECT 
    "month",
    "product_id",
    "product_name",
    SUM("product_cost") AS "total_cost",
    SUM("sale_price" - "product_cost") AS "total_profit"
  FROM "order_item_profits"
  WHERE "month" < DATE '2024-01-01'
  GROUP BY "month", "product_id", "product_name"
),
"ranked_products" AS (
  SELECT 
    "month",
    "product_id",
    "product_name",
    "total_cost",
    "total_profit",
    RANK() OVER (PARTITION BY "month" ORDER BY "total_profit" DESC) AS "rank"
  FROM "monthly_product_profits"
)
SELECT 
  "month",
  "product_id",
  "product_name",
  "total_cost",
  "total_profit"
FROM "ranked_products"
WHERE "rank" = 1
ORDER BY "month"