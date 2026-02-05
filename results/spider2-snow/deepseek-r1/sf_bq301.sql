SELECT
  a."id" AS answer_id,
  u_answer."reputation" AS answerer_reputation,
  a."score" AS answer_score,
  a."comment_count" AS answer_comment_count,
  q."tags" AS question_tags,
  q."score" AS question_score,
  q."answer_count" AS question_answer_count,
  u_asker."reputation" AS asker_reputation,
  q."view_count" AS question_view_count,
  q."comment_count" AS question_comment_count
FROM "STACKOVERFLOW"."STACKOVERFLOW"."POSTS_QUESTIONS" q
INNER JOIN "STACKOVERFLOW"."STACKOVERFLOW"."POSTS_ANSWERS" a
  ON q."accepted_answer_id" = a."id"
INNER JOIN "STACKOVERFLOW"."STACKOVERFLOW"."USERS" u_asker
  ON q."owner_user_id" = u_asker."id"
INNER JOIN "STACKOVERFLOW"."STACKOVERFLOW"."USERS" u_answer
  ON a."owner_user_id" = u_answer."id"
WHERE q."creation_date" >= 1451606400000000
  AND q."creation_date" < 1454284800000000
  AND a."creation_date" >= 1451606400000000
  AND a."creation_date" < 1454284800000000
  AND q."tags" LIKE '%javascript%'
  AND (q."tags" LIKE '%xss%' OR q."tags" LIKE '%cross-site%' OR q."tags" LIKE '%exploit%' OR q."tags" LIKE '%cybersecurity%')