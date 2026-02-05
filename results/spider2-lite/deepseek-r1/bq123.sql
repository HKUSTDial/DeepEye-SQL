WITH question_answers AS (
  SELECT 
    q.id AS question_id,
    q.creation_date AS question_date,
    MIN(a.creation_date) AS first_answer_date
  FROM `bigquery-public-data.stackoverflow.posts_questions` q
  LEFT JOIN `bigquery-public-data.stackoverflow.posts_answers` a
    ON q.id = a.parent_id
  GROUP BY q.id, q.creation_date
),
question_flags AS (
  SELECT 
    question_id,
    question_date,
    first_answer_date,
    CASE 
      WHEN TIMESTAMP_DIFF(first_answer_date, question_date, SECOND) <= 3600 
      THEN 1 
      ELSE 0 
    END AS answered_within_hour
  FROM question_answers
),
day_stats AS (
  SELECT 
    EXTRACT(DAYOFWEEK FROM question_date) AS day_of_week_num,
    FORMAT_DATE('%A', DATE(question_date)) AS day_of_week_name,
    COUNT(*) AS total_questions,
    SUM(answered_within_hour) AS answered_within_hour_count,
    (SUM(answered_within_hour) * 100.0 / COUNT(*)) AS percentage
  FROM question_flags
  GROUP BY day_of_week_num, day_of_week_name
),
ranked_days AS (
  SELECT 
    day_of_week_name,
    percentage,
    RANK() OVER (ORDER BY percentage DESC) AS rank
  FROM day_stats
)
SELECT day_of_week_name, percentage
FROM ranked_days
WHERE rank = 3