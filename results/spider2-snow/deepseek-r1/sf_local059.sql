WITH sales_2021 AS (
  SELECT "sold_quantity", "product_code"
  FROM "EDUCATION_BUSINESS"."EDUCATION_BUSINESS"."HARDWARE_FACT_SALES_MONTHLY"
  WHERE EXTRACT(YEAR FROM TO_DATE("date", 'YYYY-MM-DD')) = 2021
), product_totals AS (
  SELECT 
    p."division",
    p."product_code",
    p."product",
    SUM(s."sold_quantity") AS total_quantity
  FROM sales_2021 s
  JOIN "EDUCATION_BUSINESS"."EDUCATION_BUSINESS"."HARDWARE_DIM_PRODUCT" p 
    ON s."product_code" = p."product_code"
  GROUP BY p."division", p."product_code", p."product"
), top_products_per_division AS (
  SELECT 
    "division",
    "product_code",
    "product",
    total_quantity,
    RANK() OVER (PARTITION BY "division" ORDER BY total_quantity DESC) AS rnk
  FROM product_totals
  QUALIFY rnk <= 3
)
SELECT AVG(total_quantity) AS overall_avg_quantity
FROM top_products_per_division