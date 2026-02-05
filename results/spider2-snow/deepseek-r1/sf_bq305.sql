WITH questions AS (
  SELECT 
    "id" AS question_id,
    "owner_user_id" AS question_owner_user_id,
    COALESCE(TRY_CAST("view_count" AS INTEGER), 0) AS view_count,
    TRY_CAST("accepted_answer_id" AS NUMBER) AS accepted_answer_id
  FROM "STACKOVERFLOW"."STACKOVERFLOW"."POSTS_QUESTIONS"
), answers AS (
  SELECT 
    "id" AS answer_id,
    TRY_CAST("parent_id" AS NUMBER) AS question_id,
    "owner_user_id" AS answer_owner_user_id,
    "score" AS answer_score
  FROM "STACKOVERFLOW"."STACKOVERFLOW"."POSTS_ANSWERS"
  WHERE "owner_user_id" IS NOT NULL
), question_total_score AS (
  SELECT 
    question_id,
    SUM(answer_score) AS total_answer_score
  FROM answers
  GROUP BY question_id
), answer_rank AS (
  SELECT 
    answer_id,
    question_id,
    answer_owner_user_id,
    answer_score,
    ROW_NUMBER() OVER (PARTITION BY question_id ORDER BY answer_score DESC) AS rank
  FROM answers
), assoc_question_owner AS (
  SELECT DISTINCT
    question_owner_user_id AS user_id,
    question_id
  FROM questions
  WHERE question_owner_user_id IS NOT NULL AND question_owner_user_id != -1
), assoc_accepted_answer AS (
  SELECT DISTINCT
    a.answer_owner_user_id AS user_id,
    q.question_id
  FROM questions q
  JOIN answers a ON q.accepted_answer_id = a.answer_id
  WHERE a.answer_owner_user_id IS NOT NULL
), assoc_score_gt_5 AS (
  SELECT DISTINCT
    answer_owner_user_id AS user_id,
    question_id
  FROM answers
  WHERE answer_score > 5 AND answer_owner_user_id IS NOT NULL
), assoc_20_percent AS (
  SELECT DISTINCT
    a.answer_owner_user_id AS user_id,
    a.question_id
  FROM answers a
  JOIN question_total_score s ON a.question_id = s.question_id
  WHERE a.answer_score > 0.2 * s.total_answer_score AND a.answer_score > 0 AND a.answer_owner_user_id IS NOT NULL
), assoc_top_3 AS (
  SELECT DISTINCT
    ar.answer_owner_user_id AS user_id,
    ar.question_id
  FROM answer_rank ar
  WHERE ar.rank <= 3 AND ar.answer_owner_user_id IS NOT NULL
), all_associations AS (
  SELECT user_id, question_id FROM assoc_question_owner
  UNION
  SELECT user_id, question_id FROM assoc_accepted_answer
  UNION
  SELECT user_id, question_id FROM assoc_score_gt_5
  UNION
  SELECT user_id, question_id FROM assoc_20_percent
  UNION
  SELECT user_id, question_id FROM assoc_top_3
)
SELECT 
  u."id" AS user_id,
  u."display_name",
  SUM(q.view_count) AS total_view_count
FROM all_associations aa
INNER JOIN "STACKOVERFLOW"."STACKOVERFLOW"."USERS" u ON aa.user_id = u."id"
INNER JOIN questions q ON aa.question_id = q.question_id
GROUP BY u."id", u."display_name"
ORDER BY total_view_count DESC
LIMIT 10