WITH questions_2021 AS (
  SELECT 
    id AS question_id,
    creation_date AS question_creation,
    EXTRACT(DAYOFWEEK FROM creation_date) AS day_of_week_num,
    FORMAT_DATE('%A', DATE(creation_date)) AS day_of_week_name
  FROM `bigquery-public-data.stackoverflow.posts_questions`
  WHERE EXTRACT(YEAR FROM creation_date) = 2021
),
answer_times AS (
  SELECT 
    parent_id AS question_id,
    MIN(creation_date) AS first_answer_time
  FROM `bigquery-public-data.stackoverflow.posts_answers`
  GROUP BY parent_id
)
SELECT 
  q.day_of_week_name,
  COUNT(q.question_id) AS total_questions,
  COUNTIF(a.first_answer_time IS NOT NULL AND TIMESTAMP_DIFF(a.first_answer_time, q.question_creation, SECOND) BETWEEN 0 AND 3600) AS answered_within_1hr,
  ROUND(COUNTIF(a.first_answer_time IS NOT NULL AND TIMESTAMP_DIFF(a.first_answer_time, q.question_creation, SECOND) BETWEEN 0 AND 3600) * 100.0 / COUNT(q.question_id), 2) AS percentage
FROM questions_2021 q
LEFT JOIN answer_times a ON q.question_id = a.question_id
GROUP BY q.day_of_week_name, q.day_of_week_num
ORDER BY q.day_of_week_num