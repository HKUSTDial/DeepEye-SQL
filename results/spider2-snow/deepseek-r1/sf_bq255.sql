WITH "shell_repos" AS (
  SELECT DISTINCT l."repo_name"
  FROM "GITHUB_REPOS"."GITHUB_REPOS"."LANGUAGES" AS l,
  LATERAL FLATTEN(INPUT => l."language") AS lang
  WHERE lang.value:"name"::string = 'Shell'
), "apache_repos" AS (
  SELECT DISTINCT "repo_name"
  FROM "GITHUB_REPOS"."GITHUB_REPOS"."LICENSES"
  WHERE "license" = 'apache-2.0'
), "qualified_repos" AS (
  SELECT s."repo_name"
  FROM "shell_repos" AS s
  INNER JOIN "apache_repos" AS a ON s."repo_name" = a."repo_name"
)
SELECT COUNT(*) AS "commit_count"
FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_COMMITS" AS c
INNER JOIN "qualified_repos" AS q ON c."repo_name" = q."repo_name"
WHERE LENGTH(c."message") > 5
  AND LENGTH(c."message") < 10000
  AND NOT (
    LOWER(c."message") LIKE 'merge%' OR
    LOWER(c."message") LIKE 'update%' OR
    LOWER(c."message") LIKE 'test%'
  )