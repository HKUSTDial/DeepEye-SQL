WITH repo_primary_language AS (
  SELECT
    l."repo_name",
    lang.value:name::STRING AS language_name,
    lang.value:bytes::NUMBER AS byte_count,
    RANK() OVER (PARTITION BY l."repo_name" ORDER BY lang.value:bytes DESC) AS rnk
  FROM "GITHUB_REPOS"."GITHUB_REPOS"."LANGUAGES" AS l,
  LATERAL FLATTEN(INPUT => l."language") AS lang
  WHERE lang.value:name IS NOT NULL
),
javascript_repos AS (
  SELECT "repo_name"
  FROM repo_primary_language
  WHERE rnk = 1 AND language_name = 'JavaScript'
),
commit_counts AS (
  SELECT
    sc."repo_name",
    COUNT(*) AS commit_count
  FROM "GITHUB_REPOS"."GITHUB_REPOS"."SAMPLE_COMMITS" AS sc
  GROUP BY sc."repo_name"
)
SELECT
  jr."repo_name",
  cc.commit_count
FROM javascript_repos jr
JOIN commit_counts cc ON jr."repo_name" = cc."repo_name"
ORDER BY cc.commit_count DESC
LIMIT 2