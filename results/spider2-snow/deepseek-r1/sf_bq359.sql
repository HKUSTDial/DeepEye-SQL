SELECT js."repo_name", cc."commit_count"
FROM (
  SELECT DISTINCT "repo_name"
  FROM "GITHUB_REPOS"."GITHUB_REPOS"."LANGUAGES"
  WHERE "language"[0]::string = 'JavaScript'
) js
JOIN (
  SELECT "repo_name", COUNT(*) as "commit_count"
  FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_COMMITS"
  GROUP BY "repo_name"
) cc ON js."repo_name" = cc."repo_name"
ORDER BY cc."commit_count" DESC
LIMIT 2