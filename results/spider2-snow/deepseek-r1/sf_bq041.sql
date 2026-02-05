WITH new_users_2021 AS (
    SELECT
        "id" AS user_id,
        TO_TIMESTAMP("creation_date" / 1000000) AS user_creation_date,
        DATE_TRUNC('month', TO_TIMESTAMP("creation_date" / 1000000)) AS user_month
    FROM "STACKOVERFLOW"."STACKOVERFLOW"."USERS"
    WHERE EXTRACT(YEAR FROM TO_TIMESTAMP("creation_date" / 1000000)) = 2021
),
users_with_question AS (
    SELECT
        nu.user_id,
        MIN(TO_TIMESTAMP(pq."creation_date" / 1000000)) AS first_question_date
    FROM new_users_2021 nu
    INNER JOIN "STACKOVERFLOW"."STACKOVERFLOW"."POSTS_QUESTIONS" pq
        ON nu.user_id = pq."owner_user_id"
        AND TO_TIMESTAMP(pq."creation_date" / 1000000) BETWEEN nu.user_creation_date AND DATEADD(day, 30, nu.user_creation_date)
    GROUP BY nu.user_id
),
users_with_answer AS (
    SELECT
        uq.user_id,
        MIN(TO_TIMESTAMP(pa."creation_date" / 1000000)) AS first_answer_date
    FROM users_with_question uq
    INNER JOIN "STACKOVERFLOW"."STACKOVERFLOW"."POSTS_ANSWERS" pa
        ON uq.user_id = pa."owner_user_id"
        AND TO_TIMESTAMP(pa."creation_date" / 1000000) > uq.first_question_date
        AND TO_TIMESTAMP(pa."creation_date" / 1000000) <= DATEADD(day, 30, uq.first_question_date)
    GROUP BY uq.user_id
)
SELECT
    nu.user_month AS month,
    COUNT(DISTINCT nu.user_id) AS total_new_users,
    COUNT(DISTINCT uq.user_id) AS users_with_question,
    COUNT(DISTINCT ua.user_id) AS users_with_answer,
    ROUND(100.0 * COUNT(DISTINCT uq.user_id) / COUNT(DISTINCT nu.user_id), 2) AS pct_with_question,
    ROUND(100.0 * COUNT(DISTINCT ua.user_id) / NULLIF(COUNT(DISTINCT uq.user_id), 0), 2) AS pct_answer_given_question
FROM new_users_2021 nu
LEFT JOIN users_with_question uq ON nu.user_id = uq.user_id
LEFT JOIN users_with_answer ua ON nu.user_id = ua.user_id
GROUP BY nu.user_month
ORDER BY nu.user_month