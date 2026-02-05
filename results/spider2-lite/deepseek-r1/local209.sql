WITH `store_totals` AS (
  SELECT `o`.`store_id`, `s`.`store_name`, COUNT(*) as `total_orders`
  FROM `orders` `o`
  JOIN `stores` `s` ON `o`.`store_id` = `s`.`store_id`
  GROUP BY `o`.`store_id`, `s`.`store_name`
), `store_delivered` AS (
  SELECT `o`.`store_id`, COUNT(DISTINCT `o`.`order_id`) as `delivered_orders`
  FROM `orders` `o`
  JOIN `deliveries` `d` ON `o`.`delivery_order_id` = `d`.`delivery_order_id`
  WHERE `d`.`delivery_status` = 'DELIVERED'
  GROUP BY `o`.`store_id`
)
SELECT `t`.`store_id`, `t`.`store_name`, `t`.`total_orders`, COALESCE(`d`.`delivered_orders`, 0) as `delivered_orders`, (COALESCE(`d`.`delivered_orders`, 0) * 1.0 / `t`.`total_orders`) as `ratio`
FROM `store_totals` `t`
LEFT JOIN `store_delivered` `d` ON `t`.`store_id` = `d`.`store_id`
ORDER BY `t`.`total_orders` DESC
LIMIT 1