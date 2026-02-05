WITH first_open_users AS (
  SELECT DISTINCT 
    `user_pseudo_id`,
    DATE(TIMESTAMP_MICROS(`user_first_touch_timestamp`)) AS first_open_date
  FROM `firebase-public-project`.`analytics_153293282`.`events_*`
  WHERE EXTRACT(YEAR FROM DATE(TIMESTAMP_MICROS(`user_first_touch_timestamp`))) = 2018
    AND EXTRACT(MONTH FROM DATE(TIMESTAMP_MICROS(`user_first_touch_timestamp`))) = 9
),
uninstall_events AS (
  SELECT DISTINCT 
    `user_pseudo_id`,
    DATE(TIMESTAMP_MICROS(`event_timestamp`)) AS uninstall_date
  FROM `firebase-public-project`.`analytics_153293282`.`events_*`
  WHERE `event_name` = 'app_remove'
),
users_uninstalled_within_7 AS (
  SELECT 
    f.`user_pseudo_id`,
    f.first_open_date,
    u.uninstall_date
  FROM first_open_users f
  JOIN uninstall_events u ON f.`user_pseudo_id` = u.`user_pseudo_id`
  WHERE DATE_DIFF(u.uninstall_date, f.first_open_date, DAY) <= 7
),
crash_users AS (
  SELECT DISTINCT `user_pseudo_id`
  FROM `firebase-public-project`.`analytics_153293282`.`events_*`
  WHERE `event_name` = 'app_exception'
)
SELECT 
  COUNT(DISTINCT u.`user_pseudo_id`) AS total_users,
  COUNT(DISTINCT c.`user_pseudo_id`) AS crashed_users,
  COUNT(DISTINCT c.`user_pseudo_id`) * 100.0 / COUNT(DISTINCT u.`user_pseudo_id`) AS percentage
FROM users_uninstalled_within_7 u
LEFT JOIN crash_users c ON u.`user_pseudo_id` = c.`user_pseudo_id`