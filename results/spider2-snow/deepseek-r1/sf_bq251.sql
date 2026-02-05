WITH latest_versions AS (
  SELECT 
    "name",
    "project_urls",
    "upload_time"
  FROM "PYPI"."PYPI"."DISTRIBUTION_METADATA"
  QUALIFY ROW_NUMBER() OVER (PARTITION BY "name" ORDER BY "upload_time" DESC) = 1
),
flattened_urls AS (
  SELECT 
    lv."name",
    f.value::STRING AS url
  FROM latest_versions lv,
  LATERAL FLATTEN(INPUT => lv."project_urls") f
  WHERE f.value::STRING LIKE '%github.com%'
),
cleaned_github_urls AS (
  SELECT DISTINCT
    "name",
    cleaned_url
  FROM (
    SELECT
      fu."name",
      REGEXP_REPLACE(
        REGEXP_SUBSTR(fu.url, 'https?://github\\.com/[^/]+/[^/#?]+'),
        '/$', ''
      ) AS cleaned_url
    FROM flattened_urls fu
  )
  WHERE cleaned_url IS NOT NULL
),
download_counts AS (
  SELECT 
    "project" AS package_name,
    COUNT(*) AS download_count
  FROM "PYPI"."PYPI"."FILE_DOWNLOADS"
  GROUP BY "project"
),
ranked_packages AS (
  SELECT 
    cgu."name",
    cgu.cleaned_url,
    dc.download_count,
    ROW_NUMBER() OVER (ORDER BY dc.download_count DESC) AS rank_num
  FROM cleaned_github_urls cgu
  JOIN download_counts dc ON cgu."name" = dc.package_name
  QUALIFY rank_num <= 3
)
SELECT cleaned_url
FROM ranked_packages
ORDER BY rank_num