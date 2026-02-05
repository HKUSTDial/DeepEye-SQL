WITH "counts_2020" AS (
    SELECT 
        p."segment",
        COUNT(DISTINCT s."product_code") AS "count_2020"
    FROM "EDUCATION_BUSINESS"."EDUCATION_BUSINESS"."HARDWARE_FACT_SALES_MONTHLY" s
    INNER JOIN "EDUCATION_BUSINESS"."EDUCATION_BUSINESS"."HARDWARE_DIM_PRODUCT" p
        ON s."product_code" = p."product_code"
    WHERE s."fiscal_year" = 2020
    GROUP BY p."segment"
),
"counts_2021" AS (
    SELECT 
        p."segment",
        COUNT(DISTINCT s."product_code") AS "count_2021"
    FROM "EDUCATION_BUSINESS"."EDUCATION_BUSINESS"."HARDWARE_FACT_SALES_MONTHLY" s
    INNER JOIN "EDUCATION_BUSINESS"."EDUCATION_BUSINESS"."HARDWARE_DIM_PRODUCT" p
        ON s."product_code" = p."product_code"
    WHERE s."fiscal_year" = 2021
    GROUP BY p."segment"
)
SELECT 
    COALESCE(c0."segment", c1."segment") AS "segment",
    COALESCE(c0."count_2020", 0) AS "unique_product_count_2020"
FROM "counts_2020" c0
FULL OUTER JOIN "counts_2021" c1 
    ON c0."segment" = c1."segment"
WHERE COALESCE(c0."count_2020", 0) > 0
ORDER BY ((COALESCE(c1."count_2021", 0) - COALESCE(c0."count_2020", 0)) / NULLIF(COALESCE(c0."count_2020", 0), 0)) * 100 DESC