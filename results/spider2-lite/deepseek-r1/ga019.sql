WITH installs AS (
  SELECT 
    user_pseudo_id,
    MIN(user_first_touch_timestamp) AS install_timestamp
  FROM `firebase-public-project.analytics_153293282.events_*`
  WHERE _TABLE_SUFFIX >= '20180702'
    AND user_first_touch_timestamp IS NOT NULL
  GROUP BY user_pseudo_id
  HAVING TIMESTAMP_MICROS(MIN(user_first_touch_timestamp)) >= TIMESTAMP('2018-08-01')
    AND TIMESTAMP_MICROS(MIN(user_first_touch_timestamp)) < TIMESTAMP('2018-10-01')
),
uninstall_events AS (
  SELECT 
    user_pseudo_id,
    MIN(event_timestamp) AS uninstall_timestamp
  FROM `firebase-public-project.analytics_153293282.events_*`
  WHERE _TABLE_SUFFIX >= '20180702'
    AND event_name = 'app_remove'
  GROUP BY user_pseudo_id
),
counts AS (
  SELECT 
    COUNT(DISTINCT i.user_pseudo_id) AS total_users,
    COUNT(DISTINCT CASE 
      WHEN ue.uninstall_timestamp IS NOT NULL 
        AND TIMESTAMP_DIFF(TIMESTAMP_MICROS(ue.uninstall_timestamp), TIMESTAMP_MICROS(i.install_timestamp), DAY) <= 7 
      THEN i.user_pseudo_id 
    END) AS uninstalled_within_7
  FROM installs i
  LEFT JOIN uninstall_events ue USING (user_pseudo_id)
)
SELECT 
  (total_users - uninstalled_within_7) * 100.0 / total_users AS percentage
FROM counts