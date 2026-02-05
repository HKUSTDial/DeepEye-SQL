WITH category_totals AS (
    SELECT 
        "category",
        COUNT(*) AS total_articles
    FROM "BBC"."BBC_NEWS"."FULLTEXT"
    GROUP BY "category"
),
education_counts AS (
    SELECT 
        "category",
        COUNT(*) AS education_articles
    FROM "BBC"."BBC_NEWS"."FULLTEXT"
    WHERE "body" ILIKE '%education%' OR "title" ILIKE '%education%'
    GROUP BY "category"
)
SELECT 
    COALESCE(ct."category", ec."category") AS "category",
    COALESCE(ct.total_articles, 0) AS total_articles,
    COALESCE(ec.education_articles, 0) AS education_articles,
    ROUND(
        COALESCE(ec.education_articles, 0) * 100.0 / 
        NULLIF(COALESCE(ct.total_articles, 0), 0), 
        2
    ) AS education_percentage
FROM category_totals ct
FULL OUTER JOIN education_counts ec
    ON ct."category" = ec."category"
ORDER BY "category"