WITH user_votes AS (
    SELECT m."PostUserId", COUNT(*) AS upvote_count
    FROM "META_KAGGLE"."META_KAGGLE"."FORUMMESSAGEVOTES" v
    JOIN "META_KAGGLE"."META_KAGGLE"."FORUMMESSAGES" m ON v."ForumMessageId" = m."Id"
    WHERE EXTRACT(YEAR FROM v."VoteDate") = 2019
    GROUP BY m."PostUserId"
), avg_votes AS (
    SELECT AVG(upvote_count) AS avg_upvotes
    FROM user_votes
), differences AS (
    SELECT uv."PostUserId", uv.upvote_count, 
           ABS(uv.upvote_count - av.avg_upvotes) AS diff
    FROM user_votes uv
    CROSS JOIN avg_votes av
), min_diff AS (
    SELECT MIN(diff) AS min_diff
    FROM differences
)
SELECT u."UserName"
FROM differences d
JOIN "META_KAGGLE"."META_KAGGLE"."USERS" u ON d."PostUserId" = u."Id"
WHERE d.diff = (SELECT min_diff FROM min_diff)
ORDER BY u."UserName"
LIMIT 1