WITH non_python_repos AS (
    SELECT DISTINCT l."repo_name"
    FROM "GITHUB_REPOS"."GITHUB_REPOS"."LANGUAGES" l
    LEFT JOIN (
        SELECT DISTINCT "repo_name"
        FROM "GITHUB_REPOS"."GITHUB_REPOS"."LANGUAGES",
        LATERAL FLATTEN(INPUT => "language") lang
        WHERE lang.value:"name"::STRING ILIKE '%python%'
    ) python_repos ON l."repo_name" = python_repos."repo_name"
    WHERE python_repos."repo_name" IS NULL
)
SELECT 
    COUNT(CASE WHEN sc."sample_path" ILIKE '%readme.md%' AND sc."content" ILIKE '%Copyright (c)%' THEN 1 END) AS numerator,
    COUNT(*) AS denominator,
    numerator / denominator AS proportion
FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_CONTENTS" sc
WHERE sc."binary" = FALSE
  AND sc."sample_repo_name" IN (SELECT "repo_name" FROM non_python_repos)