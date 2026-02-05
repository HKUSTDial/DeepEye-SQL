SELECT 
    u."UserName",
    COALESCE(r."ReceivedVotes", 0) AS "TotalDistinctUpvotesReceived",
    COALESCE(g."GivenVotes", 0) AS "TotalDistinctUpvotesGiven"
FROM "META_KAGGLE"."META_KAGGLE"."USERS" u
LEFT JOIN (
    SELECT "ToUserId" AS "UserId", COUNT(DISTINCT "Id") AS "ReceivedVotes"
    FROM "META_KAGGLE"."META_KAGGLE"."FORUMMESSAGEVOTES"
    GROUP BY "ToUserId"
) r ON u."Id" = r."UserId"
LEFT JOIN (
    SELECT "FromUserId" AS "UserId", COUNT(DISTINCT "Id") AS "GivenVotes"
    FROM "META_KAGGLE"."META_KAGGLE"."FORUMMESSAGEVOTES"
    GROUP BY "FromUserId"
) g ON u."Id" = g."UserId"
WHERE r."ReceivedVotes" IS NOT NULL
ORDER BY r."ReceivedVotes" DESC
LIMIT 3