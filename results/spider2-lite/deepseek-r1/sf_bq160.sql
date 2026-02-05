WITH "parent_forum" AS (
  SELECT "Id", "Title" FROM "META_KAGGLE"."META_KAGGLE"."FORUMS" WHERE "Title" = 'General'
),
"sub_forums" AS (
  SELECT "Id", "ParentForumId" FROM "META_KAGGLE"."META_KAGGLE"."FORUMS" WHERE "ParentForumId" IN (SELECT "Id" FROM "parent_forum")
),
"earliest_topics" AS (
  SELECT "Id", "CreationDate", "Title", COALESCE("TotalViews", 0) AS "TotalViews", "ForumId"
  FROM "META_KAGGLE"."META_KAGGLE"."FORUMTOPICS"
  WHERE "ForumId" IN (SELECT "Id" FROM "sub_forums")
  ORDER BY "CreationDate" ASC
  LIMIT 5
),
"message_stats" AS (
  SELECT "ForumTopicId", COUNT(*) AS "ReplyCount", COUNT(DISTINCT "PostUserId") AS "DistinctUserReplies"
  FROM "META_KAGGLE"."META_KAGGLE"."FORUMMESSAGES"
  WHERE "ForumTopicId" IN (SELECT "Id" FROM "earliest_topics")
  GROUP BY "ForumTopicId"
),
"vote_stats" AS (
  SELECT m."ForumTopicId", COUNT(v."Id") AS "TotalUpvotes"
  FROM "META_KAGGLE"."META_KAGGLE"."FORUMMESSAGES" m
  LEFT JOIN "META_KAGGLE"."META_KAGGLE"."FORUMMESSAGEVOTES" v ON m."Id" = v."ForumMessageId"
  WHERE m."ForumTopicId" IN (SELECT "Id" FROM "earliest_topics")
  GROUP BY m."ForumTopicId"
)
SELECT et."CreationDate", et."Title", pf."Title" AS "ParentForumTitle", COALESCE(ms."ReplyCount", 0) AS "ReplyCount", COALESCE(ms."DistinctUserReplies", 0) AS "DistinctUserReplies", COALESCE(vs."TotalUpvotes", 0) AS "TotalUpvotes", et."TotalViews" AS "TotalViews"
FROM "earliest_topics" et
INNER JOIN "sub_forums" sf ON et."ForumId" = sf."Id"
INNER JOIN "parent_forum" pf ON sf."ParentForumId" = pf."Id"
LEFT JOIN "message_stats" ms ON et."Id" = ms."ForumTopicId"
LEFT JOIN "vote_stats" vs ON et."Id" = vs."ForumTopicId"
ORDER BY et."CreationDate" ASC