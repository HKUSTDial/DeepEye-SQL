WITH cohort AS (
    SELECT 
        "user_id",
        MIN(DATE(TO_TIMESTAMP("created_at" / 1000000))) AS first_purchase_date
    FROM 
        "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDERS"
    WHERE 
        "status" != 'Cancelled'
    GROUP BY 
        "user_id"
    HAVING 
        EXTRACT(YEAR FROM first_purchase_date) = 2020 
        AND EXTRACT(MONTH FROM first_purchase_date) = 1
),
returned_users AS (
    SELECT 
        c."user_id",
        CASE 
            WHEN EXISTS (
                SELECT 1 
                FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDERS" o
                WHERE o."user_id" = c."user_id"
                    AND o."returned_at" IS NOT NULL
                    AND EXTRACT(YEAR FROM DATE(TO_TIMESTAMP(o."returned_at" / 1000000))) = 2020
                    AND EXTRACT(MONTH FROM DATE(TO_TIMESTAMP(o."returned_at" / 1000000))) BETWEEN 2 AND 12
            ) THEN 1 
            ELSE 0 
        END AS returned_flag
    FROM cohort c
)
SELECT 
    SUM(returned_flag) * 1.0 / COUNT(*) AS proportion_returned
FROM returned_users