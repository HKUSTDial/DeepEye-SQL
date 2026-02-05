WITH approved_repos AS (
  SELECT DISTINCT "repo_name"
  FROM "GITHUB_REPOS_DATE"."GITHUB_REPOS"."LICENSES"
),
event_counts AS (
  SELECT 
    "repo"['name']::STRING AS repo_name,
    COUNT(*) AS combined_total
  FROM "GITHUB_REPOS_DATE"."MONTH"."_202204"
  WHERE "type" IN ('ForkEvent', 'IssuesEvent', 'WatchEvent')
    AND "repo"['name']::STRING IN (SELECT "repo_name" FROM approved_repos)
  GROUP BY "repo"['name']::STRING
)
SELECT repo_name, combined_total
FROM event_counts
ORDER BY combined_total DESC
LIMIT 1