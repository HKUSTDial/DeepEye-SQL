WITH hn_comments AS (
    SELECT "post_id"
    FROM "STACKOVERFLOW_PLUS"."HACKERNEWS"."COMMENTS_CONVOLUTION"
    WHERE "post_id" IS NOT NULL
      AND "type" = 'comment'
      AND TO_TIMESTAMP("time") >= '2014-01-01'::timestamp
    UNION ALL
    SELECT "post_id"
    FROM "STACKOVERFLOW_PLUS"."HACKERNEWS"."FULL_PARTITION_EXTRA"
    WHERE "post_id" IS NOT NULL
      AND "type" = 'comment'
      AND TO_TIMESTAMP("time") >= '2014-01-01'::timestamp
),
mention_counts AS (
    SELECT "post_id", COUNT(*) AS mention_count
    FROM hn_comments
    GROUP BY "post_id"
),
question_tags AS (
    SELECT q."id", q."tags", mc.mention_count
    FROM mention_counts mc
    INNER JOIN "STACKOVERFLOW_PLUS"."STACKOVERFLOW"."POSTS_QUESTIONS" q
        ON mc."post_id" = q."id"
    WHERE q."tags" IS NOT NULL
),
split_tags AS (
    SELECT qt."id", TRIM(t.value) AS tag, qt.mention_count
    FROM question_tags qt,
    LATERAL FLATTEN(INPUT => SPLIT(qt."tags", '|')) t
    WHERE t.value <> ''
)
SELECT tag, SUM(mention_count) AS total_mentions
FROM split_tags
GROUP BY tag
ORDER BY total_mentions DESC
LIMIT 10