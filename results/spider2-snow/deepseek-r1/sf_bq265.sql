WITH registered_2019 AS (
  SELECT "id" FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."USERS"
  WHERE EXTRACT(YEAR FROM TO_TIMESTAMP("created_at" / 1000000)) = 2019
),
purchased_2019 AS (
  SELECT DISTINCT "user_id" FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDERS"
  WHERE EXTRACT(YEAR FROM TO_TIMESTAMP("created_at" / 1000000)) = 2019
    AND "status" != 'Cancelled'
),
eligible_users AS (
  SELECT "id" FROM registered_2019
  INTERSECT
  SELECT "user_id" FROM purchased_2019
),
successful_orders AS (
  SELECT "order_id", "user_id" FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDERS"
  WHERE "status" != 'Cancelled'
    AND "user_id" IN (SELECT "id" FROM eligible_users)
),
successful_order_items AS (
  SELECT oi."order_id", oi."user_id", oi."sale_price"
  FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS" oi
  INNER JOIN successful_orders so ON oi."order_id" = so."order_id" AND oi."user_id" = so."user_id"
  WHERE oi."status" != 'Cancelled'
),
order_totals AS (
  SELECT "order_id", "user_id", SUM("sale_price") AS order_total
  FROM successful_order_items
  GROUP BY "order_id", "user_id"
),
user_aggregates AS (
  SELECT "user_id", SUM(order_total) AS total_revenue, COUNT(DISTINCT "order_id") AS num_orders
  FROM order_totals
  GROUP BY "user_id"
  HAVING num_orders > 0
),
user_aov AS (
  SELECT "user_id", total_revenue / num_orders AS avg_order_value
  FROM user_aggregates
),
ranked_users AS (
  SELECT "user_id", avg_order_value, ROW_NUMBER() OVER (ORDER BY avg_order_value DESC) AS rank_num
  FROM user_aov
)
SELECT u."email" FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."USERS" u
INNER JOIN ranked_users ru ON u."id" = ru."user_id"
WHERE ru.rank_num <= 10
ORDER BY ru.rank_num