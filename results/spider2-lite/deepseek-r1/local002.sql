WITH RECURSIVE `date_range`(`d`) AS (
  SELECT DATE('2017-01-01')
  UNION ALL
  SELECT DATE(`d`, '+1 day')
  FROM `date_range`
  WHERE `d` < '2018-08-29'
),
`toy_products` AS (
  SELECT `p`.`product_id`
  FROM `products` `p`
  JOIN `product_category_name_translation` `t` ON `p`.`product_category_name` = `t`.`product_category_name`
  WHERE `t`.`product_category_name_english` = 'toys'
),
`daily_sales` AS (
  SELECT DATE(`o`.`order_purchase_timestamp`) AS `sale_date`, SUM(`oi`.`price`) AS `total_sales`
  FROM `orders` `o`
  JOIN `order_items` `oi` ON `o`.`order_id` = `oi`.`order_id`
  WHERE `oi`.`product_id` IN (SELECT `product_id` FROM `toy_products`)
    AND DATE(`o`.`order_purchase_timestamp`) BETWEEN '2017-01-01' AND '2018-08-29'
  GROUP BY DATE(`o`.`order_purchase_timestamp`)
),
`historical_data` AS (
  SELECT `dr`.`d` AS `date`,
         (julianday(`dr`.`d`) - julianday('2017-01-01')) AS `x`,
         COALESCE(`ds`.`total_sales`, 0) AS `y`
  FROM `date_range` `dr`
  LEFT JOIN `daily_sales` `ds` ON `dr`.`d` = `ds`.`sale_date`
),
`regression` AS (
  SELECT 
    COUNT(*) AS `n`,
    SUM(`x`) AS `sum_x`,
    SUM(`y`) AS `sum_y`,
    SUM(`x` * `y`) AS `sum_xy`,
    SUM(`x` * `x`) AS `sum_x2`
  FROM `historical_data`
),
`coefficients` AS (
  SELECT 
    (`n` * `sum_xy` - `sum_x` * `sum_y`) / (`n` * `sum_x2` - `sum_x` * `sum_x`) AS `slope`,
    (`sum_y` - ((`n` * `sum_xy` - `sum_x` * `sum_y`) / (`n` * `sum_x2` - `sum_x` * `sum_x`)) * `sum_x`) / `n` AS `intercept`
  FROM `regression`
),
`prediction_date_range`(`d`) AS (
  SELECT DATE('2018-12-03')
  UNION ALL
  SELECT DATE(`d`, '+1 day')
  FROM `prediction_date_range`
  WHERE `d` < '2018-12-10'
),
`prediction_dates` AS (
  SELECT `d` AS `date`,
         (julianday(`d`) - julianday('2017-01-01')) AS `x`
  FROM `prediction_date_range`
),
`predicted_sales` AS (
  SELECT `pd`.`date`,
         `c`.`intercept` + `c`.`slope` * `pd`.`x` AS `predicted_amount`
  FROM `prediction_dates` `pd`
  CROSS JOIN `coefficients` `c`
),
`target_dates` AS (
  SELECT DATE('2018-12-05') AS `target_date` UNION ALL
  SELECT DATE('2018-12-06') UNION ALL
  SELECT DATE('2018-12-07') UNION ALL
  SELECT DATE('2018-12-08')
),
`moving_averages` AS (
  SELECT `td`.`target_date`,
         AVG(`ps`.`predicted_amount`) AS `moving_avg`
  FROM `target_dates` `td`
  JOIN `predicted_sales` `ps` ON `ps`.`date` BETWEEN DATE(`td`.`target_date`, '-2 days') AND DATE(`td`.`target_date`, '+2 days')
  GROUP BY `td`.`target_date`
)
SELECT SUM(`moving_avg`) AS `total_sum`
FROM `moving_averages`