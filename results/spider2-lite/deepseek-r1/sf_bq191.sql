WITH repo_watchers AS (
    SELECT 
        "repo":name::STRING AS repo_name,
        COUNT(DISTINCT "actor":id::STRING) AS watcher_count
    FROM "GITHUB_REPOS_DATE"."YEAR"."_2017"
    WHERE "type" = 'WatchEvent'
    GROUP BY "repo":name::STRING
    HAVING watcher_count > 300
)
SELECT DISTINCT
    rw.repo_name,
    rw.watcher_count
FROM repo_watchers rw
INNER JOIN "GITHUB_REPOS_DATE"."GITHUB_REPOS"."SAMPLE_FILES" sf
    ON rw.repo_name = sf."repo_name"
ORDER BY rw.watcher_count DESC
LIMIT 2