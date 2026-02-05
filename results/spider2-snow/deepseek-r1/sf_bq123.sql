WITH questions_with_day AS (
  SELECT 
    "id",
    "creation_date",
    DAYNAME(TO_TIMESTAMP("creation_date" / 1000000)) AS day_name
  FROM "STACKOVERFLOW"."STACKOVERFLOW"."POSTS_QUESTIONS"
),
question_earliest_answer AS (
  SELECT 
    q."id",
    q."creation_date" AS q_creation,
    q.day_name,
    MIN(a."creation_date") AS earliest_answer_creation
  FROM questions_with_day q
  LEFT JOIN "STACKOVERFLOW"."STACKOVERFLOW"."POSTS_ANSWERS" a
    ON q."id" = TRY_CAST(a."parent_id" AS NUMBER)
  GROUP BY q."id", q."creation_date", q.day_name
),
question_flags AS (
  SELECT 
    day_name,
    CASE 
      WHEN earliest_answer_creation IS NOT NULL 
        AND (earliest_answer_creation - q_creation) <= 3600000000 
      THEN 1 
      ELSE 0 
    END AS answered_within_hour
  FROM question_earliest_answer
),
day_aggregates AS (
  SELECT 
    day_name,
    COUNT(*) AS total_questions,
    SUM(answered_within_hour) AS answered_within_hour_count,
    SUM(answered_within_hour) * 100.0 / COUNT(*) AS percentage
  FROM question_flags
  GROUP BY day_name
),
ranked_days AS (
  SELECT 
    day_name,
    percentage,
    ROW_NUMBER() OVER (ORDER BY percentage DESC, day_name) AS rank_num
  FROM day_aggregates
)
SELECT day_name, percentage
FROM ranked_days
WHERE rank_num = 3