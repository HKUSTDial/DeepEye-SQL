WITH session_users AS (
  SELECT DISTINCT user_pseudo_id
  FROM `firebase-public-project.analytics_153293282.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20180702' AND '20180716'
  AND event_name = 'session_start'
),
quickplay_events AS (
  SELECT 
    user_pseudo_id,
    event_date,
    event_timestamp,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'quickplay_type') AS quickplay_type
  FROM `firebase-public-project.analytics_153293282.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20180702' AND '20180716'
  AND EXISTS (SELECT 1 FROM UNNEST(event_params) WHERE key = 'game_mode' AND value.string_value = 'quickplay')
),
initial_quickplay AS (
  SELECT 
    user_pseudo_id,
    MIN(PARSE_DATE('%Y%m%d', event_date)) AS initial_date,
    ARRAY_AGG(quickplay_type ORDER BY event_timestamp LIMIT 1)[OFFSET(0)] AS quickplay_type
  FROM quickplay_events
  WHERE user_pseudo_id IN (SELECT user_pseudo_id FROM session_users)
  GROUP BY user_pseudo_id
),
retention_check AS (
  SELECT 
    iq.user_pseudo_id,
    iq.quickplay_type,
    iq.initial_date,
    CASE WHEN EXISTS (
      SELECT 1
      FROM `firebase-public-project.analytics_153293282.events_*` e
      WHERE e.user_pseudo_id = iq.user_pseudo_id
      AND e.event_name = 'session_start'
      AND PARSE_DATE('%Y%m%d', e.event_date) = DATE_ADD(iq.initial_date, INTERVAL 14 DAY)
      AND _TABLE_SUFFIX BETWEEN '20180702' AND '20180730'
    ) THEN 1 ELSE 0 END AS retained
  FROM initial_quickplay iq
)
SELECT 
  quickplay_type,
  COUNT(DISTINCT user_pseudo_id) AS total_users,
  SUM(retained) AS retained_users,
  SAFE_DIVIDE(SUM(retained), COUNT(DISTINCT user_pseudo_id)) AS retention_rate
FROM retention_check
GROUP BY quickplay_type
ORDER BY quickplay_type