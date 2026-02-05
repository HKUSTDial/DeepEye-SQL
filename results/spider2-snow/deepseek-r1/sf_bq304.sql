WITH filtered_questions AS (
    SELECT 
        "id",
        "title",
        "body",
        "tags",
        "view_count"
    FROM 
        "STACKOVERFLOW"."STACKOVERFLOW"."POSTS_QUESTIONS"
    WHERE 
        "post_type_id" = 1
        AND (
            UPPER("title") ILIKE '%HOW%' 
            OR UPPER("body") ILIKE '%HOW%'
        )
        AND NOT (
            UPPER("title") ILIKE ANY ('%FAIL%', '%PROBLEM%', '%ERROR%', '%WRONG%', '%FIX%', '%BUG%', '%ISSUE%', '%SOLVE%', '%TROUBLE%')
            OR UPPER("body") ILIKE ANY ('%FAIL%', '%PROBLEM%', '%ERROR%', '%WRONG%', '%FIX%', '%BUG%', '%ISSUE%', '%SOLVE%', '%TROUBLE%')
        )
),
questions_with_tags AS (
    SELECT 
        fq."id",
        fq."title",
        fq."body",
        fq."tags",
        fq."view_count",
        TRIM(t.value) AS "tag"
    FROM 
        filtered_questions fq,
        LATERAL FLATTEN(INPUT => SPLIT(fq."tags", '|')) t
    WHERE 
        TRIM(t.value) IN (
            'android-layout', 'android-activity', 'android-intent', 
            'android-edittext', 'android-fragments', 'android-recyclerview', 
            'listview', 'android-actionbar', 'google-maps', 'android-asynctask'
        )
),
tag_counts AS (
    SELECT 
        "tag",
        COUNT(DISTINCT "id") AS question_count
    FROM 
        questions_with_tags
    GROUP BY 
        "tag"
    HAVING 
        COUNT(DISTINCT "id") >= 50
),
ranked_questions AS (
    SELECT 
        qwt."id",
        qwt."title",
        qwt."body",
        qwt."tags",
        qwt."view_count",
        qwt."tag",
        ROW_NUMBER() OVER (PARTITION BY qwt."tag" ORDER BY qwt."view_count" DESC) AS rank_position
    FROM 
        questions_with_tags qwt
    INNER JOIN 
        tag_counts tc ON qwt."tag" = tc."tag"
)
SELECT 
    "tag",
    "id",
    "title",
    "body",
    "tags",
    "view_count"
FROM 
    ranked_questions
WHERE 
    rank_position <= 50
ORDER BY 
    "tag",
    rank_position