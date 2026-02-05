WITH "python_repos" AS (
  SELECT DISTINCT "repo_name"
  FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_FILES"
  WHERE "path" LIKE '%.py'
),
"monthly_commits_2016" AS (
  SELECT 
    EXTRACT(MONTH FROM TO_TIMESTAMP("committer"['date']::STRING)) AS "month",
    COUNT(*) AS "commit_count"
  FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_COMMITS"
  WHERE "repo_name" IN (SELECT "repo_name" FROM "python_repos")
    AND EXTRACT(YEAR FROM TO_TIMESTAMP("committer"['date']::STRING)) = 2016
  GROUP BY EXTRACT(MONTH FROM TO_TIMESTAMP("committer"['date']::STRING))
)
SELECT COALESCE(AVG("commit_count"), 0) AS "average_commits_per_month"
FROM "monthly_commits_2016"