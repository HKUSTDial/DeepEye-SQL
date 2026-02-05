WITH months AS (
  SELECT month FROM UNNEST(GENERATE_ARRAY(1,12)) AS month
),
bourbon_sales_2022 AS (
  SELECT 
    SPLIT(zip_code, '.')[OFFSET(0)] AS cleaned_zip,
    EXTRACT(MONTH FROM date) AS month,
    sale_dollars
  FROM `bigquery-public-data.iowa_liquor_sales.sales`
  WHERE LOWER(category_name) LIKE '%bourbon%'
    AND county = 'DUBUQUE'
    AND EXTRACT(YEAR FROM date) = 2022
),
zip_total_sales AS (
  SELECT 
    cleaned_zip,
    SUM(sale_dollars) AS total_sales
  FROM bourbon_sales_2022
  GROUP BY cleaned_zip
),
ranked_zips AS (
  SELECT 
    cleaned_zip,
    total_sales,
    ROW_NUMBER() OVER (ORDER BY total_sales DESC) AS rank
  FROM zip_total_sales
),
third_zip AS (
  SELECT cleaned_zip FROM ranked_zips WHERE rank = 3
),
adult_population AS (
  SELECT 
    zipcode,
    SUM(population) AS adult_pop
  FROM `bigquery-public-data.census_bureau_usa.population_by_zip_2010`
  WHERE gender IS NULL 
    AND minimum_age IS NOT NULL 
    AND minimum_age >= 21
  GROUP BY zipcode
),
monthly_sales AS (
  SELECT 
    month,
    SUM(sale_dollars) AS monthly_total
  FROM bourbon_sales_2022
  WHERE cleaned_zip = (SELECT cleaned_zip FROM third_zip)
  GROUP BY month
)
SELECT 
  m.month,
  ROUND(COALESCE(ms.monthly_total, 0) / (SELECT adult_pop FROM adult_population WHERE zipcode = (SELECT cleaned_zip FROM third_zip)), 2) AS per_capita_sales
FROM months m
LEFT JOIN monthly_sales ms ON m.month = ms.month
ORDER BY m.month;