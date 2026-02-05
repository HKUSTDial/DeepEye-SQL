WITH page_events AS (
  SELECT 
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20201201' AND '20201231'
    AND event_dimensions.hostname = 'shop.googlemerchandisestore.com'
    AND event_name = 'page_view'
),
cleaned_paths AS (
  SELECT 
    REGEXP_REPLACE(
      COALESCE(REGEXP_EXTRACT(page_location, r'^[^:]+://[^/]+(/[^?#]*)'), '/'),
      '/+', '/') AS cleaned_path
  FROM page_events
  WHERE page_location IS NOT NULL
),
aggregated AS (
  SELECT 
    cleaned_path,
    COUNT(*) AS page_views
  FROM cleaned_paths
  GROUP BY cleaned_path
),
ranked AS (
  SELECT 
    cleaned_path,
    page_views,
    DENSE_RANK() OVER (ORDER BY page_views DESC) AS rank
  FROM aggregated
)
SELECT cleaned_path AS page_path
FROM ranked
WHERE rank = 2