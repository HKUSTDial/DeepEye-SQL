WITH user_activity AS (
  SELECT 
    user_pseudo_id,
    COUNTIF(event_name = 'page_view') as pageview_count,
    MAX(CASE WHEN ecommerce.transaction_id IS NOT NULL THEN 1 ELSE 0 END) as is_purchaser
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20201201' AND '20201231'
  GROUP BY user_pseudo_id
)
SELECT 
  AVG(CASE WHEN is_purchaser = 1 THEN pageview_count END) - AVG(CASE WHEN is_purchaser = 0 THEN pageview_count END) as avg_difference
FROM user_activity