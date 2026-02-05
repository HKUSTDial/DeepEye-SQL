WITH page_views AS (
  SELECT
    user_pseudo_id,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_202101*`
  WHERE event_name = 'page_view'
    AND _TABLE_SUFFIX BETWEEN '01' AND '31'
    AND event_date BETWEEN '20210101' AND '20210131'
),
page_counts AS (
  SELECT page_location, COUNT(*) AS view_count
  FROM page_views
  WHERE page_location IS NOT NULL
  GROUP BY page_location
),
max_page AS (
  SELECT page_location
  FROM page_counts
  ORDER BY view_count DESC
  LIMIT 1
)
SELECT COUNT(DISTINCT user_pseudo_id) AS distinct_users
FROM page_views
WHERE page_location = (SELECT page_location FROM max_page)