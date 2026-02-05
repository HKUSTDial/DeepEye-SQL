WITH monthly_totals AS (
    SELECT 
        "customer_id",
        DATE_TRUNC('month', TO_TIMESTAMP("payment_date")) AS "payment_month",
        SUM("amount") AS "monthly_total"
    FROM "SQLITE_SAKILA"."SQLITE_SAKILA"."PAYMENT"
    GROUP BY "customer_id", "payment_month"
),
monthly_changes AS (
    SELECT 
        "customer_id",
        "monthly_total" - LAG("monthly_total") OVER (PARTITION BY "customer_id" ORDER BY "payment_month") AS "monthly_change"
    FROM monthly_totals
),
avg_changes AS (
    SELECT 
        "customer_id",
        AVG("monthly_change") AS "avg_monthly_change"
    FROM monthly_changes
    WHERE "monthly_change" IS NOT NULL
    GROUP BY "customer_id"
)
SELECT 
    C."first_name",
    C."last_name"
FROM avg_changes A
JOIN "SQLITE_SAKILA"."SQLITE_SAKILA"."CUSTOMER" C
    ON A."customer_id" = C."customer_id"
ORDER BY A."avg_monthly_change" DESC
LIMIT 1