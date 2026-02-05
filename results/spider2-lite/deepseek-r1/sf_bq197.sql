WITH monthly_product_sales AS (
  SELECT 
    DATE_TRUNC('month', TO_TIMESTAMP("THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS"."created_at" / 1000000)) as month,
    "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS"."name" as product_name,
    "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS"."brand" as brand,
    "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS"."category" as category,
    COUNT(*) as total_sales,
    SUM("THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS"."sale_price") as total_revenue,
    "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDERS"."status" as order_status
  FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS"
  JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDERS"
    ON "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS"."order_id" = "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDERS"."order_id"
  JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS"
    ON "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS"."product_id" = "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS"."id"
  WHERE "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDERS"."status" = 'Complete'
    AND "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS"."brand" IS NOT NULL
    AND DATE_TRUNC('month', TO_TIMESTAMP("THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS"."created_at" / 1000000)) < '2024-07-01'
  GROUP BY month, product_name, brand, category, order_status
),
ranked_products AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY month ORDER BY total_sales DESC, total_revenue DESC) as rank
  FROM monthly_product_sales
)
SELECT 
  TO_CHAR(month, 'YYYY-MM') as month,
  product_name,
  brand,
  category,
  total_sales,
  ROUND(total_revenue, 2) as rounded_total_revenue,
  order_status
FROM ranked_products
WHERE rank = 1
ORDER BY month