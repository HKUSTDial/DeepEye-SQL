WITH "python_repos" AS (
  SELECT DISTINCT "repo_name"
  FROM "GITHUB_REPOS"."GITHUB_REPOS"."LANGUAGES"
  , LATERAL FLATTEN(INPUT => "language") AS "lang"
  WHERE LOWER("lang"."VALUE"::STRING) LIKE '%python%'
), "python_free_repos" AS (
  SELECT DISTINCT "repo_name"
  FROM "GITHUB_REPOS"."GITHUB_REPOS"."LANGUAGES"
  WHERE "repo_name" NOT IN (SELECT "repo_name" FROM "python_repos")
), "all_files" AS (
  SELECT DISTINCT "sample_repo_name", "sample_path"
  FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_CONTENTS"
  WHERE "sample_repo_name" IN (SELECT "repo_name" FROM "python_free_repos")
), "copyright_readme_files" AS (
  SELECT DISTINCT "sample_repo_name", "sample_path"
  FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_CONTENTS"
  WHERE "sample_repo_name" IN (SELECT "repo_name" FROM "python_free_repos")
    AND LOWER("sample_path") LIKE '%readme.md%'
    AND "content" ILIKE '%Copyright (c)%'
)
SELECT
  CASE
    WHEN (SELECT COUNT(*) FROM "all_files") > 0
    THEN (SELECT COUNT(*) FROM "copyright_readme_files")::FLOAT / (SELECT COUNT(*) FROM "all_files")
    ELSE 0
  END AS "proportion"