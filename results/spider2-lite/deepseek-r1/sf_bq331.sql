WITH first_messages AS (
  SELECT ft."Id" AS topic_id, fm."Id" AS message_id, fm."PostUserId"
  FROM "META_KAGGLE"."META_KAGGLE"."FORUMTOPICS" ft
  INNER JOIN "META_KAGGLE"."META_KAGGLE"."FORUMMESSAGES" fm ON ft."FirstForumMessageId" = fm."Id"
  WHERE ft."FirstForumMessageId" IS NOT NULL
),
message_scores AS (
  SELECT fm.message_id, fm."PostUserId", COUNT(DISTINCT v."FromUserId") AS message_score
  FROM first_messages fm
  LEFT JOIN "META_KAGGLE"."META_KAGGLE"."FORUMMESSAGEVOTES" v ON fm.message_id = v."ForumMessageId"
  GROUP BY fm.message_id, fm."PostUserId"
),
overall_avg AS (
  SELECT AVG(message_score) AS avg_score
  FROM message_scores
),
user_aggregated_scores AS (
  SELECT "PostUserId", SUM(message_score) AS user_score
  FROM message_scores
  GROUP BY "PostUserId"
),
top_users AS (
  SELECT "PostUserId", user_score
  FROM user_aggregated_scores
  ORDER BY user_score DESC
  LIMIT 3
)
SELECT u."UserName" AS username, ABS(tu.user_score - oa.avg_score) AS absolute_difference
FROM top_users tu
CROSS JOIN overall_avg oa
JOIN "META_KAGGLE"."META_KAGGLE"."USERS" u ON tu."PostUserId" = u."Id"
ORDER BY tu.user_score DESC