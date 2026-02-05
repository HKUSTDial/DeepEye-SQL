WITH "base_orders" AS (
  SELECT
    ROW_NUMBER() OVER (PARTITION BY c."order_id" ORDER BY c."pizza_id", c."exclusions", c."extras") AS "row_id",
    c."order_id",
    c."customer_id",
    c."pizza_id",
    c."order_time",
    pn."pizza_name"
  FROM "MODERN_DATA"."MODERN_DATA"."PIZZA_CLEAN_CUSTOMER_ORDERS" c
  JOIN "MODERN_DATA"."MODERN_DATA"."PIZZA_NAMES" pn ON c."pizza_id" = pn."pizza_id"
),
"standard_toppings" AS (
  SELECT
    pr."pizza_id",
    TRIM(t.value)::NUMBER AS "topping_id"
  FROM "MODERN_DATA"."MODERN_DATA"."PIZZA_RECIPES" pr,
  LATERAL FLATTEN(INPUT => SPLIT(pr."toppings", ',')) t
),
"extras_agg" AS (
  SELECT
    ex."order_id",
    ex."row_id",
    ex."extras" AS "topping_id",
    SUM(ex."extras_count") AS "extra_count"
  FROM "MODERN_DATA"."MODERN_DATA"."PIZZA_GET_EXTRAS" ex
  GROUP BY ex."order_id", ex."row_id", ex."extras"
),
"topping_contributions" AS (
  SELECT
    bo."row_id",
    bo."order_id",
    bo."customer_id",
    bo."pizza_name",
    bo."order_time",
    st."topping_id",
    1 AS "cnt"
  FROM "base_orders" bo
  JOIN "standard_toppings" st ON bo."pizza_id" = st."pizza_id"
  WHERE NOT EXISTS (
    SELECT 1 FROM "MODERN_DATA"."MODERN_DATA"."PIZZA_GET_EXCLUSIONS" e
    WHERE e."order_id" = bo."order_id"
      AND e."row_id" = bo."row_id"
      AND e."exclusions" = st."topping_id"
  )
  UNION ALL
  SELECT
    bo."row_id",
    bo."order_id",
    bo."customer_id",
    bo."pizza_name",
    bo."order_time",
    ex."topping_id",
    ex."extra_count" AS "cnt"
  FROM "base_orders" bo
  JOIN "extras_agg" ex ON bo."order_id" = ex."order_id" AND bo."row_id" = ex."row_id"
),
"topping_totals" AS (
  SELECT
    tc."row_id",
    tc."order_id",
    tc."customer_id",
    tc."pizza_name",
    tc."order_time",
    tc."topping_id",
    SUM(tc."cnt") AS "total_count"
  FROM "topping_contributions" tc
  GROUP BY tc."row_id", tc."order_id", tc."customer_id", tc."pizza_name", tc."order_time", tc."topping_id"
),
"topping_strings" AS (
  SELECT
    tt."row_id",
    tt."order_id",
    tt."customer_id",
    tt."pizza_name",
    tt."order_time",
    LISTAGG(
      CASE
        WHEN tt."total_count" >= 2 THEN '2x' || pt."topping_name"
        ELSE pt."topping_name"
      END,
      ', '
    ) WITHIN GROUP (ORDER BY CASE WHEN tt."total_count" >= 2 THEN 0 ELSE 1 END, pt."topping_name") AS "ingredients_list"
  FROM "topping_totals" tt
  JOIN "MODERN_DATA"."MODERN_DATA"."PIZZA_TOPPINGS" pt ON tt."topping_id" = pt."topping_id"
  GROUP BY tt."row_id", tt."order_id", tt."customer_id", tt."pizza_name", tt."order_time"
)
SELECT
  bo."row_id",
  bo."order_id",
  bo."customer_id",
  bo."pizza_name",
  bo."pizza_name" || ': ' || COALESCE(ts."ingredients_list", '') AS "final_ingredients"
FROM "base_orders" bo
LEFT JOIN "topping_strings" ts ON bo."row_id" = ts."row_id" AND bo."order_id" = ts."order_id" AND bo."pizza_name" = ts."pizza_name" AND bo."order_time" = ts."order_time"
ORDER BY bo."row_id"