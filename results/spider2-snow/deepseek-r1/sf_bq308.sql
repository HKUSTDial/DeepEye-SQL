WITH questions_2021 AS (
  SELECT 
    "id",
    "creation_date",
    TO_TIMESTAMP("creation_date" / 1000000) AS creation_timestamp,
    DAYOFWEEK(creation_timestamp) AS day_of_week_num,
    DAYNAME(creation_timestamp) AS day_of_week_name
  FROM "STACKOVERFLOW"."STACKOVERFLOW"."POSTS_QUESTIONS"
  WHERE EXTRACT(YEAR FROM TO_TIMESTAMP("creation_date" / 1000000)) = 2021
),
answers_agg AS (
  SELECT 
    "parent_id",
    MIN("creation_date") AS first_answer_time
  FROM "STACKOVERFLOW"."STACKOVERFLOW"."POSTS_ANSWERS"
  GROUP BY "parent_id"
),
questions_with_answer_flag AS (
  SELECT 
    q."id",
    q.day_of_week_name,
    q.day_of_week_num,
    CASE 
      WHEN a.first_answer_time IS NOT NULL 
           AND (a.first_answer_time - q."creation_date") <= 3600000000 
      THEN 1 
      ELSE 0 
    END AS answered_within_one_hour
  FROM questions_2021 q
  LEFT JOIN answers_agg a ON q."id" = a."parent_id"
)
SELECT 
  day_of_week_name,
  COUNT(*) AS total_questions,
  SUM(answered_within_one_hour) AS answered_within_one_hour,
  ROUND(SUM(answered_within_one_hour) * 100.0 / COUNT(*), 2) AS percentage_answered_within_one_hour
FROM questions_with_answer_flag
GROUP BY day_of_week_name, day_of_week_num
ORDER BY day_of_week_num