WITH sales_with_index AS (
    SELECT 
        "product_id",
        TO_DATE("mth", 'YYYY-MM-DD') AS "month_date",
        "qty" AS "sales",
        ROW_NUMBER() OVER (PARTITION BY "product_id" ORDER BY TO_DATE("mth", 'YYYY-MM-DD')) AS "month_index"
    FROM "ORACLE_SQL"."ORACLE_SQL"."MONTHLY_SALES"
),
cma_data AS (
    SELECT 
        "product_id",
        "month_date",
        "sales",
        "month_index",
        SUM("sales") OVER (PARTITION BY "product_id" ORDER BY "month_index" ROWS BETWEEN 5 PRECEDING AND 6 FOLLOWING) AS "sum1",
        SUM("sales") OVER (PARTITION BY "product_id" ORDER BY "month_index" ROWS BETWEEN 6 PRECEDING AND 5 FOLLOWING) AS "sum2"
    FROM sales_with_index
),
ratio_data AS (
    SELECT 
        "product_id",
        "month_date",
        "sales",
        ("sum1" + "sum2") / 24.0 AS "cma",
        "sales" / NULLIF(("sum1" + "sum2") / 24.0, 0) AS "ratio"
    FROM cma_data
    WHERE "month_index" BETWEEN 7 AND 30
)
SELECT 
    p."ID" AS "product_id",
    p."NAME" AS "product_name"
FROM "ORACLE_SQL"."ORACLE_SQL"."PRODUCTS" p
WHERE p."ID" IN (
    SELECT "product_id"
    FROM ratio_data
    WHERE EXTRACT(YEAR FROM "month_date") = 2017
    GROUP BY "product_id"
    HAVING MIN("ratio") > 2 AND COUNT(DISTINCT "month_date") = 12
)