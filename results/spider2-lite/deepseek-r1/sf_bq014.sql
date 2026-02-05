WITH "first_orders" AS (
    SELECT "user_id", MIN("created_at") AS "first_order_time"
    FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDERS"
    WHERE "status" != 'Cancelled' AND "returned_at" IS NULL
    GROUP BY "user_id"
), "first_order_categories" AS (
    SELECT DISTINCT fo."user_id", p."category"
    FROM "first_orders" fo
    INNER JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDERS" o ON fo."user_id" = o."user_id" AND fo."first_order_time" = o."created_at"
    INNER JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS" oi ON o."order_id" = oi."order_id"
    INNER JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS" p ON oi."product_id" = p."id"
    WHERE o."status" != 'Cancelled' AND o."returned_at" IS NULL
      AND oi."status" != 'Cancelled' AND oi."returned_at" IS NULL
), "category_counts" AS (
    SELECT "category", COUNT(DISTINCT "user_id") AS "customer_count"
    FROM "first_order_categories"
    GROUP BY "category"
), "top_category" AS (
    SELECT "category"
    FROM "category_counts"
    ORDER BY "customer_count" DESC
    LIMIT 1
)
SELECT SUM(oi."sale_price") AS "revenue"
FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS" oi
INNER JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS" p ON oi."product_id" = p."id"
WHERE oi."status" != 'Cancelled' AND oi."returned_at" IS NULL
  AND p."category" = (SELECT "category" FROM "top_category")