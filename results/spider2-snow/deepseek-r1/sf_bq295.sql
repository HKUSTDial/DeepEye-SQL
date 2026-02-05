SELECT r."repo_name", r."watch_count"
FROM (
  SELECT "repo"['name']::text AS "repo_name", COUNT(*) AS "watch_count"
  FROM "GITHUB_REPOS_DATE"."YEAR"."_2017"
  WHERE "type" = 'WatchEvent'
  GROUP BY "repo"['name']::text
) r
JOIN (
  SELECT DISTINCT "sample_repo_name"
  FROM "GITHUB_REPOS_DATE"."GITHUB_REPOS"."SAMPLE_CONTENTS"
  WHERE "binary" = FALSE
    AND "sample_path" LIKE '%.py'
    AND "size" < 15000
    AND "content" LIKE '%def %'
) p
ON r."repo_name" = p."sample_repo_name"
ORDER BY r."watch_count" DESC
LIMIT 3