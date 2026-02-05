SELECT COUNT(*) AS event_count
FROM "GITHUB_REPOS_DATE"."DAY"."_20230118" e
INNER JOIN (
    SELECT DISTINCT "repo_name"
    FROM "GITHUB_REPOS_DATE"."GITHUB_REPOS"."LANGUAGES"
    , LATERAL FLATTEN(INPUT => "language") f
    WHERE f.value::STRING = 'JavaScript'
) js_repos ON e."repo":"name"::STRING = js_repos."repo_name"
WHERE e."type" = 'PullRequestEvent'