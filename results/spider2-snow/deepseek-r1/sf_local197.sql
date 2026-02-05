WITH top_customers AS (
    SELECT "customer_id"
    FROM "SQLITE_SAKILA"."SQLITE_SAKILA"."PAYMENT"
    GROUP BY "customer_id"
    ORDER BY SUM("amount") DESC
    LIMIT 10
), monthly_totals AS (
    SELECT p."customer_id", DATE_TRUNC('month', TO_TIMESTAMP(p."payment_date")) AS payment_month, SUM(p."amount") AS monthly_total
    FROM "SQLITE_SAKILA"."SQLITE_SAKILA"."PAYMENT" p
    INNER JOIN top_customers tc ON p."customer_id" = tc."customer_id"
    GROUP BY p."customer_id", payment_month
), with_lag AS (
    SELECT "customer_id", payment_month, monthly_total, LAG(monthly_total) OVER (PARTITION BY "customer_id" ORDER BY payment_month) AS prev_month_total, monthly_total - LAG(monthly_total) OVER (PARTITION BY "customer_id" ORDER BY payment_month) AS diff
    FROM monthly_totals
), max_diff AS (
    SELECT "customer_id", payment_month, diff
    FROM with_lag
    WHERE diff IS NOT NULL
    ORDER BY ABS(diff) DESC
    LIMIT 1
)
SELECT c."first_name", c."last_name", TO_CHAR(md.payment_month, 'YYYY-MM') AS month, ROUND(ABS(md.diff), 2) AS difference
FROM max_diff md
JOIN "SQLITE_SAKILA"."SQLITE_SAKILA"."CUSTOMER" c ON md."customer_id" = c."customer_id"