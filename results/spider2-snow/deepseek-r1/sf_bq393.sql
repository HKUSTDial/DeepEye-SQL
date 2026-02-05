WITH user_activity_months AS (
    SELECT DISTINCT
        "by" AS user_id,
        DATE_TRUNC('month', TO_TIMESTAMP("time")) AS activity_month
    FROM "HACKER_NEWS"."HACKER_NEWS"."FULL"
    WHERE "time" IS NOT NULL
        AND "by" IS NOT NULL
        AND TO_TIMESTAMP("time") < TIMESTAMP '2024-09-11'
),
user_first_last AS (
    SELECT
        user_id,
        MIN(activity_month) AS first_activity_month,
        MAX(activity_month) AS last_activity_month
    FROM user_activity_months
    GROUP BY user_id
),
qualifying_users AS (
    SELECT
        user_id,
        first_activity_month,
        last_activity_month
    FROM user_first_last
    WHERE last_activity_month < DATE '2024-09-01'
),
user_month_numbers AS (
    SELECT
        user_id,
        DATEDIFF('month', first_activity_month, last_activity_month) + 2 AS month_number
    FROM qualifying_users
)
SELECT
    user_id,
    month_number
FROM user_month_numbers
ORDER BY month_number DESC, user_id
LIMIT 1