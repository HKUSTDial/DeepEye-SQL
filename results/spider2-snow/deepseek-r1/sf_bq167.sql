WITH vote_counts AS (
    SELECT 
        "FromUserId" AS voter_id,
        "ToUserId" AS recipient_id,
        COUNT(DISTINCT "ForumMessageId") AS direct_votes
    FROM "META_KAGGLE"."META_KAGGLE"."FORUMMESSAGEVOTES"
    WHERE "FromUserId" != "ToUserId"
    GROUP BY "FromUserId", "ToUserId"
),
paired_counts AS (
    SELECT 
        v1.voter_id,
        v1.recipient_id,
        v1.direct_votes AS received_upvotes,
        COALESCE(v2.direct_votes, 0) AS returned_upvotes
    FROM vote_counts v1
    LEFT JOIN vote_counts v2 
        ON v1.voter_id = v2.recipient_id AND v1.recipient_id = v2.voter_id
)
SELECT 
    u1."UserName" AS voter_username,
    u2."UserName" AS recipient_username,
    p.received_upvotes,
    p.returned_upvotes
FROM paired_counts p
JOIN "META_KAGGLE"."META_KAGGLE"."USERS" u1 ON p.voter_id = u1."Id"
JOIN "META_KAGGLE"."META_KAGGLE"."USERS" u2 ON p.recipient_id = u2."Id"
ORDER BY p.received_upvotes DESC, p.returned_upvotes DESC
LIMIT 1