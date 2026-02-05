SELECT e.repo_name, e.watch_count
FROM (
    SELECT "repo":"name"::STRING AS repo_name, COUNT(*) AS watch_count
    FROM "GITHUB_REPOS_DATE"."YEAR"."_2017"
    WHERE "type" = 'WatchEvent'
    GROUP BY "repo":"name"::STRING
) e
WHERE e.repo_name IN (
    SELECT DISTINCT "sample_repo_name"
    FROM "GITHUB_REPOS_DATE"."GITHUB_REPOS"."SAMPLE_CONTENTS"
    WHERE "binary" = FALSE
        AND "size" < 15000
        AND "sample_path" LIKE '%.py'
        AND "content" LIKE '%def %'
)
ORDER BY e.watch_count DESC
LIMIT 3