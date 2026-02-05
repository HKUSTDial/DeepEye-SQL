WITH order_categories AS (
  SELECT DISTINCT oi."order_id", p."product_category_name"
  FROM "BRAZILIAN_E_COMMERCE"."BRAZILIAN_E_COMMERCE"."OLIST_ORDER_ITEMS" oi
  JOIN "BRAZILIAN_E_COMMERCE"."BRAZILIAN_E_COMMERCE"."OLIST_PRODUCTS" p ON oi."product_id" = p."product_id"
), payments_with_category AS (
  SELECT op."order_id", op."payment_type", oc."product_category_name"
  FROM "BRAZILIAN_E_COMMERCE"."BRAZILIAN_E_COMMERCE"."OLIST_ORDER_PAYMENTS" op
  JOIN order_categories oc ON op."order_id" = oc."order_id"
), counts_per_category_payment AS (
  SELECT "product_category_name", "payment_type", COUNT(*) AS "payment_count"
  FROM payments_with_category
  GROUP BY "product_category_name", "payment_type"
), ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY "product_category_name" ORDER BY "payment_count" DESC, "payment_type") AS "rn"
  FROM counts_per_category_payment
), top_payment_per_category AS (
  SELECT "product_category_name", "payment_type", "payment_count"
  FROM ranked
  WHERE "rn" = 1
)
SELECT AVG("payment_count") AS "avg_top_payment_count"
FROM top_payment_per_category