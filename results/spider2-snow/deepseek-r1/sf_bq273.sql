WITH monthly_profits AS (
  SELECT
    DATE_TRUNC('month', TO_TIMESTAMP(oi."delivered_at"::NUMBER / 1000000)) AS "delivery_month",
    SUM(oi."sale_price" - ii."cost") AS "monthly_profit"
  FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS" oi
  JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDERS" o ON oi."order_id" = o."order_id"
  JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."USERS" u ON oi."user_id" = u."id"
  JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."INVENTORY_ITEMS" ii ON oi."inventory_item_id" = ii."id"
  WHERE oi."status" = 'Complete'
    AND u."traffic_source" = 'Facebook'
    AND TO_TIMESTAMP(o."created_at"::NUMBER / 1000000) BETWEEN '2022-08-01' AND '2023-11-30'
    AND oi."delivered_at" IS NOT NULL
  GROUP BY "delivery_month"
),
profit_changes AS (
  SELECT
    "delivery_month",
    "monthly_profit",
    LAG("monthly_profit") OVER (ORDER BY "delivery_month") AS "previous_month_profit",
    "monthly_profit" - LAG("monthly_profit") OVER (ORDER BY "delivery_month") AS "profit_increase"
  FROM monthly_profits
)
SELECT
  TO_CHAR("delivery_month", 'Month YYYY') AS "month",
  "profit_increase"
FROM profit_changes
WHERE "profit_increase" IS NOT NULL
  AND "delivery_month" BETWEEN '2022-08-01' AND '2023-11-30'
ORDER BY "profit_increase" DESC
LIMIT 5