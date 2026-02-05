WITH user_answers AS (
    SELECT 
        "id" AS answer_id,
        "parent_id" AS question_id
    FROM 
        "STACKOVERFLOW"."STACKOVERFLOW"."POSTS_ANSWERS"
    WHERE 
        "owner_user_id" = 1908967
        AND "creation_date" < 1528329600000000
),
answer_votes AS (
    SELECT 
        "post_id" AS answer_id,
        COUNT(CASE WHEN "vote_type_id" = 2 THEN 1 END) AS upvotes,
        COUNT(CASE WHEN "vote_type_id" = 1 THEN 1 END) AS accepted
    FROM 
        "STACKOVERFLOW"."STACKOVERFLOW"."VOTES"
    WHERE 
        "vote_type_id" IN (1, 2)
    GROUP BY 
        "post_id"
),
answer_scores AS (
    SELECT 
        ua.answer_id,
        ua.question_id,
        COALESCE(av.upvotes, 0) AS upvotes,
        COALESCE(av.accepted, 0) AS accepted,
        (10 * COALESCE(av.upvotes, 0) + 15 * COALESCE(av.accepted, 0)) AS contribution
    FROM 
        user_answers ua
        LEFT JOIN answer_votes av ON ua.answer_id = av.answer_id
),
question_tags AS (
    SELECT 
        "id" AS question_id,
        "tags"
    FROM 
        "STACKOVERFLOW"."STACKOVERFLOW"."POSTS_QUESTIONS"
),
tags_split AS (
    SELECT 
        ascores.answer_id,
        ascores.contribution,
        TRIM(t.value, '<>') AS tag_name
    FROM 
        answer_scores ascores
        INNER JOIN question_tags q ON ascores.question_id = q.question_id
        CROSS JOIN TABLE(FLATTEN(SPLIT(q."tags", '><'))) t
    WHERE 
        tag_name IS NOT NULL AND tag_name != ''
)
SELECT 
    tag_name,
    SUM(contribution) AS total_score
FROM 
    tags_split
GROUP BY 
    tag_name
ORDER BY 
    total_score DESC
LIMIT 10