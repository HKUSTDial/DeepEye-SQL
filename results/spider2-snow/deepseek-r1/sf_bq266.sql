WITH "months_2020" AS (
    SELECT DISTINCT DATE_TRUNC('month', TO_TIMESTAMP("created_at" / 1000000)) AS "month"
    FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS"
    WHERE EXTRACT(YEAR FROM TO_TIMESTAMP("created_at" / 1000000)) = 2020
), "product_monthly_sales" AS (
    SELECT 
        "oi"."product_id",
        DATE_TRUNC('month', TO_TIMESTAMP("oi"."created_at" / 1000000)) AS "month"
    FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS" "oi"
    WHERE EXTRACT(YEAR FROM TO_TIMESTAMP("oi"."created_at" / 1000000)) = 2020
    GROUP BY "oi"."product_id", "month"
), "products_in_all_months" AS (
    SELECT "pms"."product_id"
    FROM "product_monthly_sales" "pms"
    GROUP BY "pms"."product_id"
    HAVING COUNT(DISTINCT "pms"."month") = (SELECT COUNT(*) FROM "months_2020")
), "product_profits" AS (
    SELECT 
        "p"."id" AS "product_id",
        "p"."name" AS "product_name",
        "p"."retail_price" - "p"."cost" AS "profit"
    FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."PRODUCTS" "p"
    WHERE "p"."id" IN (SELECT "product_id" FROM "products_in_all_months")
), "min_profit" AS (
    SELECT MIN("profit") AS "min_profit"
    FROM "product_profits"
)
SELECT 
    "pp"."product_name",
    "pms"."month"
FROM "product_profits" "pp"
INNER JOIN "product_monthly_sales" "pms" ON "pp"."product_id" = "pms"."product_id"
INNER JOIN "min_profit" "mp" ON "pp"."profit" = "mp"."min_profit"
ORDER BY "pms"."month", "pp"."product_name"