WITH "city_sales_q4_2019" AS (
    SELECT c."cust_city_id", SUM(s."amount_sold") AS "total_sales"
    FROM "COMPLEX_ORACLE"."COMPLEX_ORACLE"."SALES" s
    JOIN "COMPLEX_ORACLE"."COMPLEX_ORACLE"."CUSTOMERS" c ON s."cust_id" = c."cust_id"
    JOIN "COMPLEX_ORACLE"."COMPLEX_ORACLE"."COUNTRIES" co ON c."country_id" = co."country_id"
    JOIN "COMPLEX_ORACLE"."COMPLEX_ORACLE"."TIMES" t ON s."time_id" = t."time_id"
    WHERE co."country_name" = 'United States'
      AND s."promo_id" = 999
      AND t."calendar_quarter_id" = 1772
    GROUP BY c."cust_city_id"
),
"city_sales_q4_2020" AS (
    SELECT c."cust_city_id", SUM(s."amount_sold") AS "total_sales"
    FROM "COMPLEX_ORACLE"."COMPLEX_ORACLE"."SALES" s
    JOIN "COMPLEX_ORACLE"."COMPLEX_ORACLE"."CUSTOMERS" c ON s."cust_id" = c."cust_id"
    JOIN "COMPLEX_ORACLE"."COMPLEX_ORACLE"."COUNTRIES" co ON c."country_id" = co."country_id"
    JOIN "COMPLEX_ORACLE"."COMPLEX_ORACLE"."TIMES" t ON s."time_id" = t."time_id"
    WHERE co."country_name" = 'United States'
      AND s."promo_id" = 999
      AND t."calendar_quarter_id" = 1776
    GROUP BY c."cust_city_id"
),
"qualified_cities" AS (
    SELECT a."cust_city_id"
    FROM "city_sales_q4_2019" a
    JOIN "city_sales_q4_2020" b ON a."cust_city_id" = b."cust_city_id"
    WHERE b."total_sales" >= a."total_sales" * 1.20
),
"sales_data" AS (
    SELECT s."prod_id", t."calendar_quarter_id", s."amount_sold"
    FROM "COMPLEX_ORACLE"."COMPLEX_ORACLE"."SALES" s
    JOIN "COMPLEX_ORACLE"."COMPLEX_ORACLE"."CUSTOMERS" c ON s."cust_id" = c."cust_id"
    JOIN "COMPLEX_ORACLE"."COMPLEX_ORACLE"."COUNTRIES" co ON c."country_id" = co."country_id"
    JOIN "COMPLEX_ORACLE"."COMPLEX_ORACLE"."TIMES" t ON s."time_id" = t."time_id"
    WHERE co."country_name" = 'United States'
      AND s."promo_id" = 999
      AND t."calendar_quarter_id" IN (1772, 1776)
      AND c."cust_city_id" IN (SELECT "cust_city_id" FROM "qualified_cities")
),
"product_quarter_sales" AS (
    SELECT "prod_id", "calendar_quarter_id", SUM("amount_sold") AS "product_sales"
    FROM "sales_data"
    GROUP BY "prod_id", "calendar_quarter_id"
),
"quarter_totals" AS (
    SELECT "calendar_quarter_id", SUM("amount_sold") AS "total_sales"
    FROM "sales_data"
    GROUP BY "calendar_quarter_id"
),
"product_shares" AS (
    SELECT p."prod_id", p."calendar_quarter_id", p."product_sales" / q."total_sales" AS "share"
    FROM "product_quarter_sales" p
    JOIN "quarter_totals" q ON p."calendar_quarter_id" = q."calendar_quarter_id"
),
"product_share_change" AS (
    SELECT "prod_id",
           MAX(CASE WHEN "calendar_quarter_id" = 1772 THEN "share" END) AS "share_2019",
           MAX(CASE WHEN "calendar_quarter_id" = 1776 THEN "share" END) AS "share_2020",
           "share_2020" - "share_2019" AS "share_change"
    FROM "product_shares"
    GROUP BY "prod_id"
    HAVING "share_2019" IS NOT NULL AND "share_2020" IS NOT NULL
),
"product_total_sales" AS (
    SELECT "prod_id", SUM("amount_sold") AS "total_product_sales"
    FROM "sales_data"
    GROUP BY "prod_id"
),
"product_ranked" AS (
    SELECT "prod_id", "total_product_sales",
           NTILE(5) OVER (ORDER BY "total_product_sales" DESC) AS "tile"
    FROM "product_total_sales"
)
SELECT p."prod_name"
FROM "product_share_change" sc
JOIN "product_ranked" r ON sc."prod_id" = r."prod_id"
JOIN "COMPLEX_ORACLE"."COMPLEX_ORACLE"."PRODUCTS" p ON sc."prod_id" = p."prod_id"
WHERE r."tile" = 1
ORDER BY ABS(sc."share_change") ASC
LIMIT 1