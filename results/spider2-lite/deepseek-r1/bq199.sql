WITH per_liter_prices AS (
  SELECT 
    `category_name`,
    `date`,
    (`state_bottle_retail` * 1000.0) / `bottle_volume_ml` AS per_liter_price
  FROM `bigquery-public-data.iowa_liquor_sales.sales`
  WHERE `bottle_volume_ml` IS NOT NULL AND `bottle_volume_ml` > 0
),
top_categories_2021 AS (
  SELECT 
    `category_name`,
    AVG(per_liter_price) AS avg_2021
  FROM per_liter_prices
  WHERE EXTRACT(YEAR FROM `date`) = 2021
  GROUP BY `category_name`
  ORDER BY avg_2021 DESC
  LIMIT 10
),
avg_2019 AS (
  SELECT 
    p.`category_name`,
    AVG(p.per_liter_price) AS avg_2019
  FROM per_liter_prices p
  INNER JOIN top_categories_2021 t ON p.`category_name` = t.`category_name`
  WHERE EXTRACT(YEAR FROM p.`date`) = 2019
  GROUP BY p.`category_name`
),
avg_2020 AS (
  SELECT 
    p.`category_name`,
    AVG(p.per_liter_price) AS avg_2020
  FROM per_liter_prices p
  INNER JOIN top_categories_2021 t ON p.`category_name` = t.`category_name`
  WHERE EXTRACT(YEAR FROM p.`date`) = 2020
  GROUP BY p.`category_name`
)
SELECT 
  t.`category_name`,
  a2019.avg_2019,
  a2020.avg_2020,
  t.avg_2021
FROM top_categories_2021 t
LEFT JOIN avg_2019 a2019 ON t.`category_name` = a2019.`category_name`
LEFT JOIN avg_2020 a2020 ON t.`category_name` = a2020.`category_name`
ORDER BY t.avg_2021 DESC