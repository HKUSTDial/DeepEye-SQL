SELECT COUNT(*) AS "count"
FROM "GITHUB_REPOS_DATE"."DAY"."_20230118" AS events
INNER JOIN (
  SELECT DISTINCT "repo_name"
  FROM "GITHUB_REPOS_DATE"."GITHUB_REPOS"."LANGUAGES",
       LATERAL FLATTEN(INPUT => "language") AS lang
  WHERE lang.value::STRING = 'JavaScript'
) AS js_repos ON events."repo":"name"::STRING = js_repos."repo_name"
WHERE events."type" = 'PullRequestEvent'
  AND PARSE_JSON(events."payload"):"action"::STRING = 'opened'
  AND TO_TIMESTAMP(events."created_at" / 1000000)::DATE = '2023-01-18'