WITH delivered_pizzas AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY co.order_id, co.pizza_id) AS row_id,
        co.order_id,
        co.pizza_id,
        co.exclusions,
        co.extras,
        pr.toppings AS base_toppings
    FROM `pizza_customer_orders` co
    INNER JOIN `pizza_clean_runner_orders` ro ON co.order_id = ro.order_id
    INNER JOIN `pizza_recipes` pr ON co.pizza_id = pr.pizza_id
    WHERE ro.cancellation IS NULL
),
base_split AS (
    SELECT row_id, 
           CAST(TRIM(j.value) AS INTEGER) AS topping_id
    FROM delivered_pizzas,
         json_each('[' || replace(base_toppings, ' ', '') || ']') AS j
),
exclusions_split AS (
    SELECT row_id, 
           CAST(TRIM(j.value) AS INTEGER) AS topping_id
    FROM delivered_pizzas,
         json_each(
             CASE 
                 WHEN exclusions IS NULL OR exclusions = '' THEN '[]'
                 ELSE '[' || replace(exclusions, ' ', '') || ']'
             END
         ) AS j
),
extras_split AS (
    SELECT row_id, 
           CAST(TRIM(j.value) AS INTEGER) AS topping_id
    FROM delivered_pizzas,
         json_each(
             CASE 
                 WHEN extras IS NULL OR extras = '' THEN '[]'
                 ELSE '[' || replace(extras, ' ', '') || ']'
             END
         ) AS j
),
base_minus_exclusions AS (
    SELECT b.row_id, b.topping_id
    FROM base_split b
    WHERE NOT EXISTS (
        SELECT 1 FROM exclusions_split e 
        WHERE e.row_id = b.row_id AND e.topping_id = b.topping_id
    )
),
all_toppings AS (
    SELECT row_id, topping_id FROM base_minus_exclusions
    UNION ALL
    SELECT row_id, topping_id FROM extras_split
)
SELECT pt.topping_name, COUNT(*) AS quantity
FROM all_toppings at
INNER JOIN `pizza_toppings` pt ON at.topping_id = pt.topping_id
GROUP BY pt.topping_id, pt.topping_name
ORDER BY quantity DESC, pt.topping_name