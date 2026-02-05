WITH bourbon_sales_2022 AS (
  SELECT
    SPLIT_PART("zip_code", '.', 1) AS zip_clean,
    "sale_dollars",
    "date"
  FROM "IOWA_LIQUOR_SALES_PLUS"."IOWA_LIQUOR_SALES"."SALES"
  WHERE "county" = 'DUBUQUE'
    AND ("category_name" LIKE '%BOURBON%' OR "item_description" LIKE '%BOURBON%')
    AND EXTRACT(YEAR FROM "date") = 2022
),
zip_total_sales AS (
  SELECT
    zip_clean,
    SUM("sale_dollars") AS total_sales
  FROM bourbon_sales_2022
  GROUP BY zip_clean
),
ranked_zip AS (
  SELECT
    zip_clean,
    RANK() OVER (ORDER BY total_sales DESC) AS sales_rank
  FROM zip_total_sales
),
third_zip AS (
  SELECT zip_clean FROM ranked_zip WHERE sales_rank = 3
),
adult_population AS (
  SELECT
    "zipcode",
    SUM("population") AS adult_pop
  FROM "IOWA_LIQUOR_SALES_PLUS"."CENSUS_BUREAU_USA"."POPULATION_BY_ZIP_2010"
  WHERE "minimum_age" >= 21
    AND "zipcode" IN (SELECT zip_clean FROM third_zip)
  GROUP BY "zipcode"
),
monthly_sales AS (
  SELECT
    EXTRACT(MONTH FROM "date") AS month_num,
    SUM("sale_dollars") AS monthly_total
  FROM bourbon_sales_2022
  WHERE zip_clean IN (SELECT zip_clean FROM third_zip)
  GROUP BY EXTRACT(MONTH FROM "date")
)
SELECT
  m.month_num AS month,
  m.monthly_total / p.adult_pop AS per_capita_sales
FROM monthly_sales m
CROSS JOIN adult_population p
ORDER BY m.month_num