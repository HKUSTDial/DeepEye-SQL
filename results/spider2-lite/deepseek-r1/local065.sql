SELECT 
  SUM(
    CASE 
      WHEN n.pizza_name = 'Meatlovers' THEN 12
      WHEN n.pizza_name = 'Vegetarian' THEN 10
    END +
    CASE 
      WHEN c.extras IS NULL OR c.extras = '' THEN 0
      ELSE (LENGTH(c.extras) - LENGTH(REPLACE(c.extras, ',', '')) + 1)
    END
  ) AS total_income
FROM pizza_clean_customer_orders c
INNER JOIN pizza_names n ON c.pizza_id = n.pizza_id
INNER JOIN pizza_clean_runner_orders r ON c.order_id = r.order_id
WHERE r.cancellation IS NULL