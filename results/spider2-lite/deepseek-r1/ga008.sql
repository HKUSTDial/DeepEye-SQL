WITH purchasers AS (
  SELECT DISTINCT user_pseudo_id
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20201101' AND '20201130'
    AND ecommerce.transaction_id IS NOT NULL
),
daily_user_pageviews AS (
  SELECT 
    event_date,
    user_pseudo_id,
    COUNT(*) AS user_daily_pageviews
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20201101' AND '20201130'
    AND event_name = 'page_view'
    AND user_pseudo_id IN (SELECT user_pseudo_id FROM purchasers)
  GROUP BY event_date, user_pseudo_id
)
SELECT 
  event_date,
  SUM(user_daily_pageviews) AS total_page_views,
  AVG(user_daily_pageviews) AS avg_page_views_per_user
FROM daily_user_pageviews
GROUP BY event_date
ORDER BY event_date