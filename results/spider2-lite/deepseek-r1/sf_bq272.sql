WITH valid_order_items AS (
  SELECT 
    oi."order_id",
    oi."id" AS order_item_id,
    oi."sale_price",
    oi."inventory_item_id",
    oi."product_id",
    oi."created_at" AS sale_timestamp
  FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS" oi
  JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDERS" o
    ON oi."order_id" = o."order_id"
  WHERE o."status" != 'Cancelled' 
    AND o."returned_at" IS NULL
    AND oi."status" != 'Cancelled'
    AND oi."returned_at" IS NULL
),
sales_with_cost AS (
  SELECT 
    voi."product_id",
    p."name" AS product_name,
    voi."sale_price",
    ii."cost",
    voi.sale_timestamp
  FROM valid_order_items voi
  JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."INVENTORY_ITEMS" ii
    ON voi."inventory_item_id" = ii."id"
  JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS" p
    ON voi."product_id" = p."id"
  WHERE voi.sale_timestamp IS NOT NULL
),
monthly_profit AS (
  SELECT 
    DATE_TRUNC('month', TO_TIMESTAMP(sale_timestamp / 1000000)) AS sale_month,
    "product_id",
    product_name,
    SUM("sale_price") - SUM("cost") AS profit
  FROM sales_with_cost
  WHERE TO_TIMESTAMP(sale_timestamp / 1000000) >= '2019-01-01'::TIMESTAMP
    AND TO_TIMESTAMP(sale_timestamp / 1000000) < '2022-09-01'::TIMESTAMP
  GROUP BY 1, 2, 3
),
ranked_profits AS (
  SELECT 
    sale_month,
    "product_id",
    product_name,
    profit,
    ROW_NUMBER() OVER (PARTITION BY sale_month ORDER BY profit DESC, "product_id") AS rank
  FROM monthly_profit
)
SELECT 
  TO_CHAR(sale_month, 'YYYY-MM') AS month,
  product_name,
  profit,
  rank
FROM ranked_profits
WHERE rank <= 3
ORDER BY sale_month, rank