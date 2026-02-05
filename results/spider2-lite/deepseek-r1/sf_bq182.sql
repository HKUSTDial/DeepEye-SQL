WITH repo_primary_language AS (
  SELECT
    "repo_name" AS repo_name,
    lang.value:"name"::TEXT AS primary_language
  FROM "GITHUB_REPOS_DATE"."GITHUB_REPOS"."LANGUAGES"
  , LATERAL FLATTEN(INPUT => "language") lang
  QUALIFY ROW_NUMBER() OVER (PARTITION BY "repo_name" ORDER BY lang.value:"bytes" DESC) = 1
),
events AS (
  SELECT
    "repo":"name"::TEXT AS repo_name
  FROM "GITHUB_REPOS_DATE"."DAY"."_20230118"
  WHERE "type" = 'PullRequestEvent'
)
SELECT
  rpl.primary_language,
  COUNT(*) AS total_pullrequest_events
FROM events e
JOIN repo_primary_language rpl ON e.repo_name = rpl.repo_name
GROUP BY rpl.primary_language
HAVING COUNT(*) >= 5
ORDER BY total_pullrequest_events DESC