WITH received_counts AS (
    SELECT "ToUserId" AS "UserId", COUNT(DISTINCT "Id") AS "received_count"
    FROM "META_KAGGLE"."META_KAGGLE"."FORUMMESSAGEVOTES"
    GROUP BY "ToUserId"
),
given_counts AS (
    SELECT "FromUserId" AS "UserId", COUNT(DISTINCT "Id") AS "given_count"
    FROM "META_KAGGLE"."META_KAGGLE"."FORUMMESSAGEVOTES"
    GROUP BY "FromUserId"
)
SELECT u."UserName", rc."received_count", COALESCE(gc."given_count", 0) AS "given_count"
FROM received_counts rc
INNER JOIN "META_KAGGLE"."META_KAGGLE"."USERS" u ON rc."UserId" = u."Id"
LEFT JOIN given_counts gc ON rc."UserId" = gc."UserId"
ORDER BY rc."received_count" DESC
LIMIT 3