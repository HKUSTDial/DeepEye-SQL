WITH non_canceled_orders AS (
  SELECT "order_id"
  FROM "MODERN_DATA"."MODERN_DATA"."PIZZA_RUNNER_ORDERS"
  WHERE "cancellation" IS NULL OR "cancellation" = ''
),
pizza_details AS (
  SELECT 
    CASE 
      WHEN pn."pizza_name" = 'Meatlovers' THEN 12
      WHEN pn."pizza_name" = 'Vegetarian' THEN 10
    END AS base_price,
    CASE 
      WHEN co."extras" IS NULL OR TRIM(co."extras") = '' THEN 0
      ELSE ARRAY_SIZE(SPLIT(co."extras", ','))
    END AS extras_count
  FROM "MODERN_DATA"."MODERN_DATA"."PIZZA_CLEAN_CUSTOMER_ORDERS" co
  JOIN "MODERN_DATA"."MODERN_DATA"."PIZZA_NAMES" pn 
    ON co."pizza_id" = pn."pizza_id"
  WHERE co."order_id" IN (SELECT "order_id" FROM non_canceled_orders)
    AND pn."pizza_name" IN ('Meatlovers', 'Vegetarian')
)
SELECT SUM(base_price + extras_count) AS total_income
FROM pizza_details