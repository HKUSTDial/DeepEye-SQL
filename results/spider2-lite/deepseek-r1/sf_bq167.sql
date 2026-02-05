WITH directed_counts AS (
  SELECT
    "FromUserId",
    "ToUserId",
    COUNT(DISTINCT "Id") AS vote_count
  FROM "META_KAGGLE"."META_KAGGLE"."FORUMMESSAGEVOTES"
  GROUP BY "FromUserId", "ToUserId"
)
SELECT
  u1."UserName" AS giver_username,
  u2."UserName" AS receiver_username,
  a.vote_count AS received_upvotes,
  COALESCE(b.vote_count, 0) AS returned_upvotes
FROM directed_counts a
LEFT JOIN directed_counts b ON a."FromUserId" = b."ToUserId" AND a."ToUserId" = b."FromUserId"
JOIN "META_KAGGLE"."META_KAGGLE"."USERS" u1 ON a."FromUserId" = u1."Id"
JOIN "META_KAGGLE"."META_KAGGLE"."USERS" u2 ON a."ToUserId" = u2."Id"
ORDER BY a.vote_count DESC, COALESCE(b.vote_count, 0) DESC
LIMIT 1