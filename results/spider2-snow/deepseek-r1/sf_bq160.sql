WITH parent_forum AS (
    SELECT "Id" FROM "META_KAGGLE"."META_KAGGLE"."FORUMS" WHERE "Title" = 'General'
),
sub_forums AS (
    SELECT "Id" FROM "META_KAGGLE"."META_KAGGLE"."FORUMS" WHERE "ParentForumId" IN (SELECT "Id" FROM parent_forum)
),
topics AS (
    SELECT "Id", "CreationDate", "Title", COALESCE("TotalViews", 0) AS "TotalViews"
    FROM "META_KAGGLE"."META_KAGGLE"."FORUMTOPICS"
    WHERE "ForumId" IN (SELECT "Id" FROM sub_forums)
),
topic_metrics AS (
    SELECT "ForumTopicId",
           COUNT(*) AS "ReplyCount",
           COUNT(DISTINCT "PostUserId") AS "DistinctUserRepliesCount"
    FROM "META_KAGGLE"."META_KAGGLE"."FORUMMESSAGES"
    GROUP BY "ForumTopicId"
),
topic_upvotes AS (
    SELECT fm."ForumTopicId",
           COUNT(fmv."Id") AS "TotalUpvotes"
    FROM "META_KAGGLE"."META_KAGGLE"."FORUMMESSAGES" fm
    LEFT JOIN "META_KAGGLE"."META_KAGGLE"."FORUMMESSAGEVOTES" fmv ON fm."Id" = fmv."ForumMessageId"
    GROUP BY fm."ForumTopicId"
)
SELECT t."CreationDate",
       t."Title",
       'General' AS "ParentForumTitle",
       COALESCE(tm."ReplyCount", 0) AS "ReplyCount",
       COALESCE(tm."DistinctUserRepliesCount", 0) AS "DistinctUserRepliesCount",
       COALESCE(tu."TotalUpvotes", 0) AS "TotalUpvotes",
       t."TotalViews"
FROM topics t
LEFT JOIN topic_metrics tm ON t."Id" = tm."ForumTopicId"
LEFT JOIN topic_upvotes tu ON t."Id" = tu."ForumTopicId"
ORDER BY t."CreationDate" ASC
LIMIT 5