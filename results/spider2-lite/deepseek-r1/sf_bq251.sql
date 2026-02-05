WITH latest_versions AS (
    SELECT "name", "project_urls", "home_page",
           ROW_NUMBER() OVER (PARTITION BY "name" ORDER BY "upload_time" DESC) AS rn
    FROM "PYPI"."PYPI"."DISTRIBUTION_METADATA"
),
filtered_versions AS (
    SELECT "name", "project_urls", "home_page"
    FROM latest_versions
    WHERE rn = 1
),
all_urls AS (
    SELECT "name", "home_page" AS "url" FROM filtered_versions
    UNION ALL
    SELECT fv."name", f.value::STRING AS "url"
    FROM filtered_versions fv,
         LATERAL FLATTEN(INPUT => fv."project_urls") f
    WHERE TYPEOF(f.value) = 'VARCHAR'
),
github_urls AS (
    SELECT "name",
           MIN(REGEXP_SUBSTR("url", '^(https?://github\\.com/[^/]+/[^/]+)')) AS cleaned_github_url
    FROM all_urls
    WHERE REGEXP_LIKE("url", '^https?://github\\.com/[^/]+/[^/]+')
    GROUP BY "name"
),
download_counts AS (
    SELECT "project", COUNT(*) AS download_count
    FROM "PYPI"."PYPI"."FILE_DOWNLOADS"
    GROUP BY "project"
)
SELECT g.cleaned_github_url
FROM github_urls g
INNER JOIN download_counts d ON g."name" = d."project"
ORDER BY d.download_count DESC
LIMIT 3