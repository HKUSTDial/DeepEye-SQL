WITH activity_counts AS (
  SELECT 
    "repo":"name"::text AS "repo_name",
    COUNT(DISTINCT CASE WHEN "type" = 'WatchEvent' THEN "actor":"id"::text END) AS "watchers",
    COUNT(DISTINCT CASE WHEN "type" = 'IssuesEvent' THEN "id" END) AS "issues",
    COUNT(DISTINCT CASE WHEN "type" = 'ForkEvent' THEN "id" END) AS "forks"
  FROM "GITHUB_REPOS_DATE"."MONTH"."_202204"
  WHERE "type" IN ('WatchEvent', 'IssuesEvent', 'ForkEvent')
  GROUP BY "repo":"name"::text
),
allowed_licenses AS (
  SELECT DISTINCT "repo_name"
  FROM "GITHUB_REPOS_DATE"."GITHUB_REPOS"."LICENSES"
  WHERE "license" IN ('artistic-2.0', 'isc', 'mit', 'apache-2.0')
),
py_files_master AS (
  SELECT DISTINCT "repo_name"
  FROM "GITHUB_REPOS_DATE"."GITHUB_REPOS"."SAMPLE_FILES"
  WHERE "ref" = 'refs/heads/master' AND "path" LIKE '%.py'
)
SELECT 
  a."repo_name",
  (a."watchers" + a."issues" + a."forks") AS "activity_count"
FROM activity_counts a
INNER JOIN allowed_licenses l ON a."repo_name" = l."repo_name"
INNER JOIN py_files_master p ON a."repo_name" = p."repo_name"
ORDER BY "activity_count" DESC
LIMIT 1