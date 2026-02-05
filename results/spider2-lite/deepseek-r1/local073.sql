WITH order_rows AS (
  SELECT 
    ROW_NUMBER() OVER (ORDER BY order_id, order_time) AS row_id,
    customer_id,
    order_id,
    pizza_id,
    order_time,
    exclusions,
    extras
  FROM `pizza_customer_orders`
),
standard_toppings AS (
  SELECT 
    pr.pizza_id,
    CAST(trim(j.value) AS INTEGER) AS topping_id
  FROM `pizza_recipes` pr
  JOIN json_each('[' || replace(replace(pr.toppings, ', ', ','), ' ', '') || ']') j
),
exclusions_cte AS (
  SELECT 
    ors.row_id,
    CAST(trim(j.value) AS INTEGER) AS topping_id
  FROM order_rows ors
  JOIN json_each('[' || replace(replace(ors.exclusions, ', ', ','), ' ', '') || ']') j
  WHERE ors.exclusions IS NOT NULL AND ors.exclusions != ''
),
extras_cte AS (
  SELECT 
    ors.row_id,
    CAST(trim(j.value) AS INTEGER) AS topping_id
  FROM order_rows ors
  JOIN json_each('[' || replace(replace(ors.extras, ', ', ','), ' ', '') || ']') j
  WHERE ors.extras IS NOT NULL AND ors.extras != ''
),
all_toppings AS (
  SELECT ors.row_id, st.topping_id
  FROM order_rows ors
  JOIN standard_toppings st ON ors.pizza_id = st.pizza_id
  WHERE NOT EXISTS (
    SELECT 1 FROM exclusions_cte e 
    WHERE e.row_id = ors.row_id AND e.topping_id = st.topping_id
  )
  UNION ALL
  SELECT row_id, topping_id FROM extras_cte
),
topping_counts AS (
  SELECT row_id, topping_id, COUNT(*) AS cnt
  FROM all_toppings
  GROUP BY row_id, topping_id
),
ordered_toppings AS (
  SELECT 
    tc.row_id,
    pt.topping_name,
    tc.cnt,
    CASE WHEN tc.cnt = 2 THEN 1 ELSE 2 END AS sort_group
  FROM topping_counts tc
  JOIN `pizza_toppings` pt ON tc.topping_id = pt.topping_id
  ORDER BY tc.row_id, sort_group, pt.topping_name
),
formatted_ingredients AS (
  SELECT 
    row_id,
    group_concat(
      CASE WHEN cnt = 2 THEN '2x ' || topping_name ELSE topping_name END,
      ', '
    ) AS ingredients_list
  FROM ordered_toppings
  GROUP BY row_id
)
SELECT 
  ors.row_id,
  ors.order_id,
  ors.customer_id,
  pn.pizza_name,
  pn.pizza_name || ': ' || COALESCE(fi.ingredients_list, '') AS final_ingredients
FROM order_rows ors
JOIN `pizza_names` pn ON ors.pizza_id = pn.pizza_id
LEFT JOIN formatted_ingredients fi ON ors.row_id = fi.row_id
ORDER BY ors.row_id