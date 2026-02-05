WITH monthly_product_sales AS (
  SELECT DISTINCT
    DATE_TRUNC('month', TO_TIMESTAMP_NTZ(oi."created_at" / 1000000)) AS sales_month,
    p."id" AS product_id,
    p."name" AS product_name,
    p."retail_price" - p."cost" AS profit
  FROM
    "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS" oi
  INNER JOIN
    "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS" p
    ON oi."product_id" = p."id"
  WHERE
    oi."status" = 'Complete'
    AND EXTRACT(YEAR FROM TO_TIMESTAMP_NTZ(oi."created_at" / 1000000)) = 2020
),
monthly_min_profit AS (
  SELECT
    sales_month,
    MIN(profit) AS min_profit
  FROM
    monthly_product_sales
  GROUP BY
    sales_month
)
SELECT
  TO_CHAR(mps.sales_month, 'YYYY-MM') AS month,
  mps.product_name
FROM
  monthly_product_sales mps
INNER JOIN
  monthly_min_profit mmp
  ON mps.sales_month = mmp.sales_month AND mps.profit = mmp.min_profit
ORDER BY
  mps.sales_month ASC,
  mps.product_name;