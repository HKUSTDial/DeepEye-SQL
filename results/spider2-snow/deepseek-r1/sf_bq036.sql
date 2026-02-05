SELECT COUNT(*) / 12.0 AS "avg_commits_per_month"
FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_COMMITS" c
WHERE c."repo_name" IN (
    SELECT DISTINCT l."repo_name"
    FROM "GITHUB_REPOS"."GITHUB_REPOS"."LANGUAGES" l,
    LATERAL FLATTEN(INPUT => l."language") lang
    WHERE lang.value::string = 'Python'
)
AND EXTRACT(YEAR FROM TO_DATE(c."committer":"date"::string)) = 2016