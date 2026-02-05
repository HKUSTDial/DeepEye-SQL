WITH "monthly_product_profit" AS (
    SELECT 
        DATE_TRUNC('month', TO_TIMESTAMP("THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS"."created_at" / 1000000)) AS "month",
        "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS"."product_id",
        "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS"."name" AS "product_name",
        SUM("THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS"."cost") AS "total_cost",
        SUM("THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS"."sale_price" - "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS"."cost") AS "total_profit"
    FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS"
    INNER JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS" ON "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS"."product_id" = "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS"."id"
    WHERE TO_TIMESTAMP("THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS"."created_at" / 1000000) < '2024-01-01'::TIMESTAMP
    GROUP BY 1, 2, 3
)
SELECT 
    "month",
    "product_id",
    "product_name",
    "total_cost",
    "total_profit"
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY "month" ORDER BY "total_profit" DESC) AS "rn"
    FROM "monthly_product_profit"
) "ranked"
WHERE "rn" = 1
ORDER BY "month"