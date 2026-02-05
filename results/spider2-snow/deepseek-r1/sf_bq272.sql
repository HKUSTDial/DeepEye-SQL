WITH "filtered_order_items" AS (
  SELECT 
    "oi"."order_id",
    "oi"."id" AS "order_item_id",
    "oi"."sale_price",
    "oi"."product_id",
    "oi"."inventory_item_id",
    "oi"."created_at"
  FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS" AS "oi"
  INNER JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDERS" AS "o" 
    ON "oi"."order_id" = "o"."order_id"
  WHERE "o"."status" != 'Cancelled'
    AND "o"."returned_at" IS NULL
    AND "oi"."returned_at" IS NULL
    AND "oi"."status" != 'Cancelled'
),
"item_costs" AS (
  SELECT 
    "foi".*,
    "ii"."cost"
  FROM "filtered_order_items" AS "foi"
  INNER JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."INVENTORY_ITEMS" AS "ii" 
    ON "foi"."inventory_item_id" = "ii"."id"
),
"item_with_product" AS (
  SELECT 
    "ic".*,
    "p"."name" AS "product_name"
  FROM "item_costs" AS "ic"
  INNER JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS" AS "p" 
    ON "ic"."product_id" = "p"."id"
),
"monthly_profit" AS (
  SELECT 
    TO_CHAR(DATE_TRUNC('month', TO_TIMESTAMP("created_at" / 1000000)), 'YYYY-MM') AS "sale_month",
    "product_id",
    "product_name",
    SUM("sale_price") - SUM("cost") AS "profit"
  FROM "item_with_product"
  WHERE TO_TIMESTAMP("created_at" / 1000000) >= '2019-01-01'::TIMESTAMP
    AND TO_TIMESTAMP("created_at" / 1000000) < '2022-09-01'::TIMESTAMP
  GROUP BY 1, 2, 3
),
"ranked" AS (
  SELECT 
    "sale_month",
    "product_name",
    "profit",
    ROW_NUMBER() OVER (PARTITION BY "sale_month" ORDER BY "profit" DESC) AS "rank"
  FROM "monthly_profit"
)
SELECT 
  "sale_month",
  "product_name",
  "profit",
  "rank"
FROM "ranked"
WHERE "rank" <= 3
ORDER BY "sale_month", "rank"