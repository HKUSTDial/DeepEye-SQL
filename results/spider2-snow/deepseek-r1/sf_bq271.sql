SELECT
  EXTRACT(MONTH FROM TO_TIMESTAMP("orders"."created_at" / 1000000)) AS "month",
  "users"."country" AS "country",
  "inventory_items"."product_department" AS "product_department",
  "inventory_items"."product_category" AS "product_category",
  COUNT(DISTINCT "orders"."order_id") AS "number_of_orders",
  COUNT(DISTINCT "orders"."user_id") AS "number_of_unique_purchasers",
  SUM("inventory_items"."product_retail_price" - "inventory_items"."cost") AS "profit"
FROM
  "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDERS" AS "orders"
JOIN
  "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."USERS" AS "users"
  ON "orders"."user_id" = "users"."id"
JOIN
  "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS" AS "order_items"
  ON "orders"."order_id" = "order_items"."order_id"
JOIN
  "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."INVENTORY_ITEMS" AS "inventory_items"
  ON "order_items"."inventory_item_id" = "inventory_items"."id"
WHERE
  EXTRACT(YEAR FROM TO_TIMESTAMP("orders"."created_at" / 1000000)) = 2021
  AND EXTRACT(YEAR FROM TO_TIMESTAMP("users"."created_at" / 1000000)) = 2021
  AND EXTRACT(YEAR FROM TO_TIMESTAMP("inventory_items"."created_at" / 1000000)) = 2021
GROUP BY
  "month",
  "users"."country",
  "inventory_items"."product_department",
  "inventory_items"."product_category"
ORDER BY
  "month",
  "users"."country",
  "inventory_items"."product_department",
  "inventory_items"."product_category"