WITH all_users AS (
  SELECT DISTINCT user_pseudo_id
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20201201' AND '20201231'
),
engaged_sessions AS (
  SELECT 
    user_pseudo_id,
    (SELECT ep.value.int_value FROM UNNEST(event_params) ep WHERE ep.key = 'ga_session_id') AS ga_session_id
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20201201' AND '20201231'
    AND EXISTS (SELECT 1 FROM UNNEST(event_params) ep WHERE ep.key = 'session_engaged' 
                AND (ep.value.string_value = '1' OR ep.value.int_value = 1))
),
engaged_counts AS (
  SELECT user_pseudo_id, COUNT(DISTINCT ga_session_id) AS cnt
  FROM engaged_sessions
  GROUP BY user_pseudo_id
)
SELECT AVG(COALESCE(cnt, 0)) AS avg_engaged_sessions_per_user
FROM all_users
LEFT JOIN engaged_counts ON all_users.user_pseudo_id = engaged_counts.user_pseudo_id