WITH repo_primary_lang AS (
  SELECT 
    "repo_name",
    lang.value:"name"::TEXT as "primary_language",
    ROW_NUMBER() OVER (PARTITION BY "repo_name" ORDER BY lang.value:"bytes"::INT DESC) as rn
  FROM "GITHUB_REPOS_DATE"."GITHUB_REPOS"."LANGUAGES"
  JOIN LATERAL FLATTEN(INPUT => "language") as lang
),
pull_request_counts AS (
  SELECT 
    "repo":"name"::TEXT as "repo_name",
    COUNT(*) as "event_count"
  FROM "GITHUB_REPOS_DATE"."DAY"."_20230118"
  WHERE "type" = 'PullRequestEvent'
    AND TO_DATE(TO_TIMESTAMP("created_at" / 1000000)) = '2023-01-18'
  GROUP BY "repo":"name"::TEXT
)
SELECT 
  rpl."primary_language",
  SUM(prc."event_count") as "total_pullrequest_events"
FROM repo_primary_lang rpl
INNER JOIN pull_request_counts prc ON rpl."repo_name" = prc."repo_name"
WHERE rpl.rn = 1
GROUP BY rpl."primary_language"
HAVING SUM(prc."event_count") >= 5
ORDER BY "total_pullrequest_events" DESC