WITH raw_hn AS (
    SELECT 
        "id" AS comment_id,
        REGEXP_SUBSTR("text", 'stackoverflow\\.com/(questions|q)/(\\d+)', 1, 1, 'i', 2) AS so_question_id_str
    FROM "STACKOVERFLOW_PLUS"."HACKERNEWS"."COMMENTS_CONVOLUTION"
    WHERE "type" = 'comment'
        AND "time" >= 1388534400
        AND "text" ILIKE '%stackoverflow.com/%'
    UNION ALL
    SELECT 
        "id",
        REGEXP_SUBSTR("text", 'stackoverflow\\.com/(questions|q)/(\\d+)', 1, 1, 'i', 2)
    FROM "STACKOVERFLOW_PLUS"."HACKERNEWS"."FULL_PARTITION_EXTRA"
    WHERE "type" = 'comment'
        AND "time" >= 1388534400
        AND "text" ILIKE '%stackoverflow.com/%'
    UNION ALL
    SELECT 
        "id",
        REGEXP_SUBSTR("text", 'stackoverflow\\.com/(questions|q)/(\\d+)', 1, 1, 'i', 2)
    FROM "STACKOVERFLOW_PLUS"."HACKERNEWS"."FULL_201510"
    WHERE "type" = 'comment'
        AND "time" >= 1388534400
        AND "text" ILIKE '%stackoverflow.com/%'
    UNION ALL
    SELECT 
        "id",
        REGEXP_SUBSTR("text", 'stackoverflow\\.com/(questions|q)/(\\d+)', 1, 1, 'i', 2)
    FROM "STACKOVERFLOW_PLUS"."HACKERNEWS"."COMMENTSV2"
    WHERE "type" = 'comment'
        AND "time" >= 1388534400
        AND "text" ILIKE '%stackoverflow.com/%'
    UNION ALL
    SELECT 
        "id",
        REGEXP_SUBSTR("text", 'stackoverflow\\.com/(questions|q)/(\\d+)', 1, 1, 'i', 2)
    FROM "STACKOVERFLOW_PLUS"."HACKERNEWS"."FULL_PARTITIONED"
    WHERE "type" = 'comment'
        AND "time" >= 1388534400
        AND "text" ILIKE '%stackoverflow.com/%'
    UNION ALL
    SELECT 
        "id",
        REGEXP_SUBSTR("text", 'stackoverflow\\.com/(questions|q)/(\\d+)', 1, 1, 'i', 2)
    FROM "STACKOVERFLOW_PLUS"."HACKERNEWS"."STORIESV2"
    WHERE "type" = 'comment'
        AND "time" >= 1388534400
        AND "text" ILIKE '%stackoverflow.com/%'
    UNION ALL
    SELECT 
        "id",
        REGEXP_SUBSTR("text", 'stackoverflow\\.com/(questions|q)/(\\d+)', 1, 1, 'i', 2)
    FROM "STACKOVERFLOW_PLUS"."HACKERNEWS"."COMMENTS"
    WHERE "time" >= 1388534400
        AND "text" ILIKE '%stackoverflow.com/%'
),
hn_dedup AS (
    SELECT DISTINCT comment_id, so_question_id_str
    FROM raw_hn
    WHERE so_question_id_str IS NOT NULL
),
so_joined AS (
    SELECT 
        hn.comment_id,
        q."tags"
    FROM hn_dedup hn
    INNER JOIN "STACKOVERFLOW_PLUS"."STACKOVERFLOW"."POSTS_QUESTIONS" q
        ON q."id" = TRY_TO_NUMBER(hn.so_question_id_str)
),
tags_split AS (
    SELECT 
        t.value AS tag,
        comment_id
    FROM so_joined,
    LATERAL SPLIT_TO_TABLE(so_joined."tags", '|') t
)
SELECT 
    tag,
    COUNT(*) AS mention_count
FROM tags_split
GROUP BY tag
ORDER BY mention_count DESC
LIMIT 10