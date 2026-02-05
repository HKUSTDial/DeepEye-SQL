WITH seller_metrics AS (
  SELECT
    "seller_id",
    COUNT(*) AS "total_quantity",
    SUM("price") AS "total_sales",
    AVG("price") AS "avg_item_price"
  FROM "ELECTRONIC_SALES"."ELECTRONIC_SALES"."ORDER_ITEMS"
  GROUP BY "seller_id"
),
seller_orders AS (
  SELECT DISTINCT "seller_id", "order_id"
  FROM "ELECTRONIC_SALES"."ELECTRONIC_SALES"."ORDER_ITEMS"
),
seller_reviews AS (
  SELECT
    so."seller_id",
    AVG(orev."review_score") AS "avg_review_score"
  FROM seller_orders so
  JOIN "ELECTRONIC_SALES"."ELECTRONIC_SALES"."ORDERS" o ON so."order_id" = o."order_id"
  JOIN "ELECTRONIC_SALES"."ELECTRONIC_SALES"."ORDER_REVIEWS" orev ON o."order_id" = orev."order_id"
  GROUP BY so."seller_id"
),
seller_packing AS (
  SELECT
    so."seller_id",
    AVG(DATEDIFF('hour', TRY_TO_TIMESTAMP(o."order_approved_at"), TRY_TO_TIMESTAMP(o."order_delivered_carrier_date"))) AS "avg_packing_hours"
  FROM seller_orders so
  JOIN "ELECTRONIC_SALES"."ELECTRONIC_SALES"."ORDERS" o ON so."order_id" = o."order_id"
  WHERE TRY_TO_TIMESTAMP(o."order_approved_at") IS NOT NULL AND TRY_TO_TIMESTAMP(o."order_delivered_carrier_date") IS NOT NULL
  GROUP BY so."seller_id"
),
seller_category_counts AS (
  SELECT
    oi."seller_id",
    p."product_category_name",
    COUNT(*) AS "item_count"
  FROM "ELECTRONIC_SALES"."ELECTRONIC_SALES"."ORDER_ITEMS" oi
  JOIN "ELECTRONIC_SALES"."ELECTRONIC_SALES"."PRODUCTS" p ON oi."product_id" = p."product_id"
  GROUP BY oi."seller_id", p."product_category_name"
),
seller_top_category AS (
  SELECT
    ranked."seller_id",
    t."product_category_name_english" AS "top_category_english"
  FROM (
    SELECT
      "seller_id",
      "product_category_name",
      "item_count",
      ROW_NUMBER() OVER (PARTITION BY "seller_id" ORDER BY "item_count" DESC) AS rn
    FROM seller_category_counts
  ) ranked
  JOIN "ELECTRONIC_SALES"."ELECTRONIC_SALES"."PRODUCT_CATEGORY_NAME_TRANSLATION" t ON ranked."product_category_name" = t."product_category_name"
  WHERE ranked.rn = 1
)
SELECT
  s."seller_id",
  s."seller_state",
  s."seller_city",
  sm."total_quantity",
  sm."total_sales",
  sm."avg_item_price",
  sr."avg_review_score",
  sp."avg_packing_hours",
  stc."top_category_english"
FROM "ELECTRONIC_SALES"."ELECTRONIC_SALES"."SELLERS" s
JOIN seller_metrics sm ON s."seller_id" = sm."seller_id"
LEFT JOIN seller_reviews sr ON s."seller_id" = sr."seller_id"
LEFT JOIN seller_packing sp ON s."seller_id" = sp."seller_id"
LEFT JOIN seller_top_category stc ON s."seller_id" = stc."seller_id"
WHERE sm."total_quantity" > 100
ORDER BY sm."total_sales" DESC