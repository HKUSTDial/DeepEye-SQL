WITH "user_upvotes" AS (
    SELECT
        "ToUserId" AS "UserId",
        COUNT(*) AS "TotalUpvotes"
    FROM
        "META_KAGGLE"."META_KAGGLE"."FORUMMESSAGEVOTES"
    WHERE
        "VoteDate" BETWEEN '2019-01-01' AND '2019-12-31'
    GROUP BY
        "ToUserId"
),
"avg_upvotes" AS (
    SELECT
        AVG("TotalUpvotes") AS "AvgUpvotes"
    FROM
        "user_upvotes"
),
"differences" AS (
    SELECT
        "UserId",
        "TotalUpvotes",
        ABS("TotalUpvotes" - "AvgUpvotes") AS "Diff"
    FROM
        "user_upvotes"
    CROSS JOIN
        "avg_upvotes"
),
"min_diff" AS (
    SELECT
        MIN("Diff") AS "MinDiff"
    FROM
        "differences"
),
"candidates" AS (
    SELECT
        "differences"."UserId",
        "differences"."TotalUpvotes",
        "differences"."Diff"
    FROM
        "differences"
    JOIN
        "min_diff" ON "differences"."Diff" = "min_diff"."MinDiff"
)
SELECT
    "USERS"."UserName"
FROM
    "candidates"
JOIN
    "META_KAGGLE"."META_KAGGLE"."USERS" ON "candidates"."UserId" = "USERS"."Id"
ORDER BY
    "USERS"."UserName" ASC
LIMIT 1