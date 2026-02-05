WITH delivered_orders AS (
  SELECT "order_id"
  FROM "MODERN_DATA"."MODERN_DATA"."PIZZA_CLEAN_RUNNER_ORDERS"
  WHERE "cancellation" = ''
), pizza_orders AS (
  SELECT c."order_id", c."pizza_id", c."extras", c."exclusions"
  FROM "MODERN_DATA"."MODERN_DATA"."PIZZA_CLEAN_CUSTOMER_ORDERS" c
  INNER JOIN delivered_orders d ON c."order_id" = d."order_id"
), base_toppings AS (
  SELECT po."order_id", po."pizza_id", CAST(TRIM(t.value) AS INTEGER) AS topping_id
  FROM pizza_orders po
  INNER JOIN "MODERN_DATA"."MODERN_DATA"."PIZZA_RECIPES" pr ON po."pizza_id" = pr."pizza_id"
  , LATERAL FLATTEN(INPUT => SPLIT(pr."toppings", ',')) t
  WHERE TRIM(t.value) != ''
), extras_toppings AS (
  SELECT po."order_id", po."pizza_id", CAST(TRIM(e.value) AS INTEGER) AS topping_id
  FROM pizza_orders po
  , LATERAL FLATTEN(INPUT => SPLIT(po."extras", ',')) e
  WHERE po."extras" IS NOT NULL AND po."extras" != '' AND TRIM(e.value) != ''
), exclusions_toppings AS (
  SELECT po."order_id", po."pizza_id", CAST(TRIM(ex.value) AS INTEGER) AS topping_id
  FROM pizza_orders po
  , LATERAL FLATTEN(INPUT => SPLIT(po."exclusions", ',')) ex
  WHERE po."exclusions" IS NOT NULL AND po."exclusions" != '' AND TRIM(ex.value) != ''
), all_toppings AS (
  SELECT topping_id, 1 AS contribution FROM base_toppings
  UNION ALL
  SELECT topping_id, 1 AS contribution FROM extras_toppings
  UNION ALL
  SELECT topping_id, -1 AS contribution FROM exclusions_toppings
)
SELECT pt."topping_name", SUM(at.contribution) AS total_quantity
FROM all_toppings at
INNER JOIN "MODERN_DATA"."MODERN_DATA"."PIZZA_TOPPINGS" pt ON at.topping_id = pt."topping_id"
GROUP BY pt."topping_id", pt."topping_name"
HAVING SUM(at.contribution) > 0
ORDER BY pt."topping_name"