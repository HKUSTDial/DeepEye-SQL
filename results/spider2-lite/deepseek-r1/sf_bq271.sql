SELECT
  EXTRACT(MONTH FROM TO_TIMESTAMP(o."created_at" / 1000000)) AS "month",
  u."country",
  i."product_department",
  i."product_category",
  COUNT(DISTINCT o."order_id") AS "number_of_orders",
  COUNT(DISTINCT o."user_id") AS "number_of_unique_purchasers",
  SUM(i."product_retail_price" - i."cost") AS "profit"
FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDERS" o
INNER JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."USERS" u
  ON o."user_id" = u."id"
INNER JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS" oi
  ON o."order_id" = oi."order_id" AND o."user_id" = oi."user_id"
INNER JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."INVENTORY_ITEMS" i
  ON oi."inventory_item_id" = i."id"
WHERE EXTRACT(YEAR FROM TO_TIMESTAMP(o."created_at" / 1000000)) = 2021
  AND EXTRACT(YEAR FROM TO_TIMESTAMP(u."created_at" / 1000000)) = 2021
  AND EXTRACT(YEAR FROM TO_TIMESTAMP(i."created_at" / 1000000)) = 2021
GROUP BY
  EXTRACT(MONTH FROM TO_TIMESTAMP(o."created_at" / 1000000)),
  u."country",
  i."product_department",
  i."product_category"
ORDER BY
  "month",
  u."country",
  i."product_department",
  i."product_category"