WITH sales_2022 AS (
  SELECT 
    item_number,
    SUM(sale_dollars) AS revenue_2022
  FROM `bigquery-public-data.iowa_liquor_sales.sales`
  WHERE EXTRACT(YEAR FROM date) = 2022
  GROUP BY item_number
),
sales_2023 AS (
  SELECT 
    item_number,
    SUM(sale_dollars) AS revenue_2023,
    ANY_VALUE(item_description) AS item_description
  FROM `bigquery-public-data.iowa_liquor_sales.sales`
  WHERE EXTRACT(YEAR FROM date) = 2023
  GROUP BY item_number
)
SELECT 
  s23.item_number,
  s23.item_description,
  s22.revenue_2022,
  s23.revenue_2023,
  (s23.revenue_2023 - s22.revenue_2022) / s22.revenue_2022 * 100 AS growth_pct
FROM sales_2022 s22
INNER JOIN sales_2023 s23 ON s22.item_number = s23.item_number
WHERE s22.revenue_2022 != 0
ORDER BY growth_pct DESC
LIMIT 5