WITH first_orders AS (
  SELECT "order_id", "user_id"
  FROM (
    SELECT "order_id", "user_id", "created_at",
           ROW_NUMBER() OVER (PARTITION BY "user_id" ORDER BY "created_at") AS rn
    FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDERS"
    WHERE "status" != 'Cancelled' AND "returned_at" IS NULL
  ) WHERE rn = 1
),
user_categories AS (
  SELECT DISTINCT fo."user_id", p."category"
  FROM first_orders fo
  JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS" oi ON fo."order_id" = oi."order_id"
  JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS" p ON oi."product_id" = p."id"
  WHERE oi."status" != 'Cancelled' AND oi."returned_at" IS NULL
),
category_counts AS (
  SELECT "category", COUNT(*) AS "customer_count"
  FROM user_categories
  GROUP BY "category"
),
top_category AS (
  SELECT "category"
  FROM category_counts
  ORDER BY "customer_count" DESC
  LIMIT 1
)
SELECT SUM(oi."sale_price") AS "revenue"
FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS" oi
JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDERS" o ON oi."order_id" = o."order_id"
JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS" p ON oi."product_id" = p."id"
WHERE p."category" = (SELECT "category" FROM top_category)
  AND o."status" != 'Cancelled' AND o."returned_at" IS NULL
  AND oi."status" != 'Cancelled' AND oi."returned_at" IS NULL