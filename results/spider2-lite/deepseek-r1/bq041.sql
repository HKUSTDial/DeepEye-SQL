WITH users_2021 AS (
  SELECT 
    id AS user_id,
    creation_date AS user_creation_date,
    EXTRACT(MONTH FROM creation_date) AS month
  FROM `bigquery-public-data.stackoverflow.users`
  WHERE EXTRACT(YEAR FROM creation_date) = 2021
),
users_with_questions AS (
  SELECT 
    u.user_id,
    u.user_creation_date,
    u.month,
    MIN(q.creation_date) AS first_question_date,
    CASE WHEN MIN(q.creation_date) IS NOT NULL THEN 1 ELSE 0 END AS has_question_within_30d
  FROM users_2021 u
  LEFT JOIN `bigquery-public-data.stackoverflow.posts_questions` q
    ON u.user_id = q.owner_user_id
    AND q.creation_date BETWEEN u.user_creation_date 
        AND TIMESTAMP_ADD(u.user_creation_date, INTERVAL 30 DAY)
  GROUP BY u.user_id, u.user_creation_date, u.month
),
users_with_answers_after_first_question AS (
  SELECT 
    q.user_id,
    MAX(CASE WHEN a.creation_date > q.first_question_date 
             AND a.creation_date <= TIMESTAMP_ADD(q.first_question_date, INTERVAL 30 DAY)
             THEN 1 ELSE 0 END) AS has_answer_after_first_question
  FROM users_with_questions q
  LEFT JOIN `bigquery-public-data.stackoverflow.posts_answers` a
    ON q.user_id = a.owner_user_id
    AND a.creation_date > q.first_question_date
    AND a.creation_date <= TIMESTAMP_ADD(q.first_question_date, INTERVAL 30 DAY)
  WHERE q.has_question_within_30d = 1
  GROUP BY q.user_id
),
combined_data AS (
  SELECT 
    wq.month,
    wq.user_id,
    wq.has_question_within_30d,
    COALESCE(wa.has_answer_after_first_question, 0) AS has_answer_after_first_question
  FROM users_with_questions wq
  LEFT JOIN users_with_answers_after_first_question wa
    ON wq.user_id = wa.user_id
)
SELECT 
  month,
  COUNT(user_id) AS total_new_users,
  100.0 * SUM(has_question_within_30d) / COUNT(user_id) AS pct_with_question,
  100.0 * SUM(has_answer_after_first_question) / NULLIF(SUM(has_question_within_30d), 0) AS pct_answer_given_question
FROM combined_data
GROUP BY month
ORDER BY month