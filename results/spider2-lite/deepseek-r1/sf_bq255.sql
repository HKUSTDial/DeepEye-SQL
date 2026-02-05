WITH shell_repos AS (
  SELECT DISTINCT "repo_name"
  FROM "GITHUB_REPOS"."GITHUB_REPOS"."LANGUAGES",
  LATERAL FLATTEN(INPUT => "language") lang
  WHERE lang.value = 'Shell'
),
apache_repos AS (
  SELECT DISTINCT "repo_name"
  FROM "GITHUB_REPOS"."GITHUB_REPOS"."LICENSES"
  WHERE "license" = 'apache-2.0'
),
target_repos AS (
  SELECT s."repo_name"
  FROM shell_repos s
  INNER JOIN apache_repos a ON s."repo_name" = a."repo_name"
)
SELECT COUNT(*) 
FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_COMMITS" c
INNER JOIN target_repos t ON c."repo_name" = t."repo_name"
WHERE LENGTH(c."message") > 5 
  AND LENGTH(c."message") < 10000
  AND NOT (
    STARTSWITH(LOWER(TRIM(c."message")), 'merge') 
    OR STARTSWITH(LOWER(TRIM(c."message")), 'update')
    OR STARTSWITH(LOWER(TRIM(c."message")), 'test')
  )