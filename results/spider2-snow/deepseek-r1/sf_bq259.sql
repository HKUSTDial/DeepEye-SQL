WITH purchases AS (
    SELECT 
        "user_id",
        TO_TIMESTAMP("created_at" / 1000000) AS purchase_time,
        DATE_TRUNC('month', TO_TIMESTAMP("created_at" / 1000000)) AS purchase_month
    FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDERS"
    WHERE "status" != 'Cancelled'
        AND TO_TIMESTAMP("created_at" / 1000000) <= '2022-12-31 23:59:59'::TIMESTAMP
),
first_purchase AS (
    SELECT 
        "user_id",
        MIN(purchase_month) AS first_purchase_month
    FROM purchases
    GROUP BY "user_id"
),
user_purchases_with_offset AS (
    SELECT 
        p."user_id",
        p.purchase_month,
        fp.first_purchase_month,
        (YEAR(p.purchase_month) - YEAR(fp.first_purchase_month)) * 12 + (MONTH(p.purchase_month) - MONTH(fp.first_purchase_month)) AS month_offset
    FROM purchases p
    JOIN first_purchase fp ON p."user_id" = fp."user_id"
),
user_cohort_behavior AS (
    SELECT 
        "user_id",
        first_purchase_month,
        MAX(CASE WHEN month_offset = 0 THEN 1 ELSE 0 END) AS has_month0,
        MAX(CASE WHEN month_offset = 1 THEN 1 ELSE 0 END) AS has_month1,
        MAX(CASE WHEN month_offset = 2 THEN 1 ELSE 0 END) AS has_month2,
        MAX(CASE WHEN month_offset = 3 THEN 1 ELSE 0 END) AS has_month3
    FROM user_purchases_with_offset
    WHERE month_offset BETWEEN 0 AND 3
    GROUP BY "user_id", first_purchase_month
),
cohort_aggregation AS (
    SELECT 
        first_purchase_month AS cohort_month,
        COUNT("user_id") AS cohort_size,
        SUM(has_month0) AS users_month0,
        SUM(has_month1) AS users_month1,
        SUM(has_month2) AS users_month2,
        SUM(has_month3) AS users_month3
    FROM user_cohort_behavior
    WHERE first_purchase_month <= '2022-09-01'::DATE
    GROUP BY first_purchase_month
)
SELECT 
    cohort_month,
    (users_month0 * 100.0 / cohort_size) AS pct_month0,
    (users_month1 * 100.0 / cohort_size) AS pct_month1,
    (users_month2 * 100.0 / cohort_size) AS pct_month2,
    (users_month3 * 100.0 / cohort_size) AS pct_month3
FROM cohort_aggregation
ORDER BY cohort_month