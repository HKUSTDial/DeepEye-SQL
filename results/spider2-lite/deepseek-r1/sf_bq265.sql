WITH users_2019 AS (
    SELECT "id", "email"
    FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."USERS"
    WHERE EXTRACT(YEAR FROM TO_TIMESTAMP("created_at" / 1000000)) = 2019
),
purchases_2019 AS (
    SELECT "user_id", "order_id", "sale_price"
    FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS"
    WHERE EXTRACT(YEAR FROM TO_TIMESTAMP("created_at" / 1000000)) = 2019
),
user_aov AS (
    SELECT 
        "user_id",
        SUM("sale_price") AS "total_revenue",
        COUNT(DISTINCT "order_id") AS "order_count",
        SUM("sale_price") / COUNT(DISTINCT "order_id") AS "aov"
    FROM purchases_2019
    GROUP BY "user_id"
)
SELECT 
    u."email"
FROM users_2019 u
INNER JOIN user_aov a ON u."id" = a."user_id"
ORDER BY a."aov" DESC
LIMIT 10