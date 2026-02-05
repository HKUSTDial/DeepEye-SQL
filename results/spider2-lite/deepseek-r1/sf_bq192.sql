WITH licensed_repos AS (
  SELECT DISTINCT "repo_name"
  FROM "GITHUB_REPOS_DATE"."GITHUB_REPOS"."LICENSES"
  WHERE "license" IN ('artistic-2.0', 'isc', 'mit', 'apache-2.0')
), python_repos AS (
  SELECT DISTINCT "repo_name"
  FROM "GITHUB_REPOS_DATE"."GITHUB_REPOS"."SAMPLE_FILES"
  WHERE "ref" = 'refs/heads/master' AND "path" LIKE '%.py'
), event_counts AS (
  SELECT 
    "repo":name::string AS "repo_name",
    COUNT(DISTINCT CASE WHEN "type" = 'WatchEvent' THEN "actor":id::string END) AS "watchers",
    COUNT(DISTINCT CASE WHEN "type" = 'ForkEvent' THEN "id" END) AS "forks",
    COUNT(DISTINCT CASE WHEN "type" = 'IssuesEvent' THEN "id" END) AS "issues"
  FROM "GITHUB_REPOS_DATE"."MONTH"."_202204"
  WHERE "type" IN ('WatchEvent', 'ForkEvent', 'IssuesEvent')
  GROUP BY "repo":name::string
)
SELECT 
  lr."repo_name",
  COALESCE(ec."watchers", 0) + COALESCE(ec."forks", 0) + COALESCE(ec."issues", 0) AS "count"
FROM licensed_repos lr
INNER JOIN python_repos pr ON lr."repo_name" = pr."repo_name"
LEFT JOIN event_counts ec ON lr."repo_name" = ec."repo_name"
ORDER BY "count" DESC
LIMIT 1