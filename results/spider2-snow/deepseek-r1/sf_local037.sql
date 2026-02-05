WITH distinct_categories_per_order AS (
  SELECT DISTINCT
    oi."order_id",
    p."product_category_name"
  FROM "BRAZILIAN_E_COMMERCE"."BRAZILIAN_E_COMMERCE"."OLIST_ORDER_ITEMS" oi
  JOIN "BRAZILIAN_E_COMMERCE"."BRAZILIAN_E_COMMERCE"."OLIST_PRODUCTS" p
    ON oi."product_id" = p."product_id"
),
category_payment_counts AS (
  SELECT
    dc."product_category_name",
    pay."payment_type",
    COUNT(*) AS "payment_count"
  FROM distinct_categories_per_order dc
  JOIN "BRAZILIAN_E_COMMERCE"."BRAZILIAN_E_COMMERCE"."OLIST_ORDER_PAYMENTS" pay
    ON dc."order_id" = pay."order_id"
  GROUP BY dc."product_category_name", pay."payment_type"
),
category_dominant_payment AS (
  SELECT
    "product_category_name",
    "payment_type",
    "payment_count",
    ROW_NUMBER() OVER (
      PARTITION BY "product_category_name"
      ORDER BY "payment_count" DESC, "payment_type"
    ) AS "rn"
  FROM category_payment_counts
)
SELECT
  trans."product_category_name_english" AS "category_english",
  dom."payment_type",
  dom."payment_count"
FROM category_dominant_payment dom
JOIN "BRAZILIAN_E_COMMERCE"."BRAZILIAN_E_COMMERCE"."PRODUCT_CATEGORY_NAME_TRANSLATION" trans
  ON dom."product_category_name" = trans."product_category_name"
WHERE dom."rn" = 1
ORDER BY dom."payment_count" DESC
LIMIT 3