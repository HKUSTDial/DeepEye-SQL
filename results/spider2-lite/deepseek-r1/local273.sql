WITH `orderlines_details` AS (
  SELECT 
    `ol`.`product_id`,
    `ol`.`qty` AS `required_qty`,
    `o`.`ordered`,
    SUM(`ol`.`qty`) OVER (PARTITION BY `ol`.`product_id` ORDER BY `o`.`ordered`, `ol`.`id`) AS `cumulative_demand`,
    COALESCE(SUM(`ol`.`qty`) OVER (PARTITION BY `ol`.`product_id` ORDER BY `o`.`ordered`, `ol`.`id` ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) AS `cumulative_demand_before`
  FROM `orderlines` `ol`
  JOIN `orders` `o` ON `ol`.`order_id` = `o`.`id`
),
`inventory_totals` AS (
  SELECT `product_id`, SUM(`qty`) AS `total_supply`
  FROM `inventory`
  GROUP BY `product_id`
),
`orderline_pick` AS (
  SELECT 
    `od`.`product_id`,
    `od`.`required_qty`,
    COALESCE(`it`.`total_supply`, 0) AS `total_supply`,
    MAX(0, MIN(`od`.`cumulative_demand`, COALESCE(`it`.`total_supply`, 0)) - `od`.`cumulative_demand_before`) AS `picked_qty`
  FROM `orderlines_details` `od`
  LEFT JOIN `inventory_totals` `it` ON `od`.`product_id` = `it`.`product_id`
),
`orderline_percentage` AS (
  SELECT 
    `product_id`,
    CASE 
      WHEN `required_qty` = 0 THEN NULL
      ELSE `picked_qty` * 1.0 / `required_qty`
    END AS `pick_pct`
  FROM `orderline_pick`
)
SELECT 
  `p`.`name` AS `product_name`,
  AVG(`op`.`pick_pct`) AS `avg_pick_percentage`
FROM `orderline_percentage` `op`
JOIN `products` `p` ON `op`.`product_id` = `p`.`id`
GROUP BY `p`.`name`
ORDER BY `p`.`name`