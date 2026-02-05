WITH users_7day AS (
  SELECT DISTINCT user_pseudo_id
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20201231' AND '20210107'
    AND event_timestamp >= 1609459200000000
    AND event_timestamp <= 1610063999000000
    AND EXISTS (
      SELECT 1 
      FROM UNNEST(event_params) 
      WHERE key = 'engagement_time_msec' 
        AND value.int_value > 0
    )
),
users_2day AS (
  SELECT DISTINCT user_pseudo_id
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20210106' AND '20210107'
    AND event_timestamp >= 1609891200000000
    AND event_timestamp <= 1610063999000000
    AND EXISTS (
      SELECT 1 
      FROM UNNEST(event_params) 
      WHERE key = 'engagement_time_msec' 
        AND value.int_value > 0
    )
)
SELECT COUNT(DISTINCT user_pseudo_id) AS distinct_user_count
FROM users_7day
WHERE user_pseudo_id NOT IN (SELECT user_pseudo_id FROM users_2day)