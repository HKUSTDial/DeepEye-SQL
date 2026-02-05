WITH delivered_orders AS (
    SELECT o.`order_id`, c.`customer_city`, p.`payment_value`
    FROM `olist_orders` o
    JOIN `olist_customers` c ON o.`customer_id` = c.`customer_id`
    JOIN `olist_order_payments` p ON o.`order_id` = p.`order_id`
    WHERE o.`order_status` = 'delivered'
), city_totals AS (
    SELECT `customer_city`,
           SUM(`payment_value`) AS `total_payment`,
           COUNT(DISTINCT `order_id`) AS `total_orders`
    FROM delivered_orders
    GROUP BY `customer_city`
), lowest_five AS (
    SELECT `customer_city`, `total_payment`, `total_orders`
    FROM city_totals
    ORDER BY `total_payment` ASC
    LIMIT 5
)
SELECT AVG(`total_payment`) AS `avg_total_payment`,
       AVG(`total_orders`) AS `avg_total_orders`
FROM lowest_five