WITH all_sessions AS (
  SELECT 
    user_pseudo_id,
    event_date
  FROM `firebase-public-project.analytics_153293282.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20180709' AND '20181002'
    AND event_name = 'session_start'
),
new_user_sessions AS (
  SELECT 
    user_pseudo_id,
    event_date,
    user_first_touch_timestamp
  FROM `firebase-public-project.analytics_153293282.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20180709' AND '20181002'
    AND event_name = 'session_start'
    AND FORMAT_DATE('%Y%m%d', DATE(TIMESTAMP_MICROS(user_first_touch_timestamp))) = event_date
),
user_first_session AS (
  SELECT 
    user_pseudo_id,
    MIN(event_date) AS first_session_date
  FROM new_user_sessions
  GROUP BY user_pseudo_id
),
cohorts AS (
  SELECT 
    user_pseudo_id,
    DATE_TRUNC(DATE(PARSE_DATE('%Y%m%d', first_session_date)), WEEK(MONDAY)) AS cohort_week_start
  FROM user_first_session
)
SELECT 
  cohort_week_start,
  COUNT(DISTINCT c.user_pseudo_id) AS cohort_size,
  COUNT(DISTINCT CASE 
    WHEN DATE(PARSE_DATE('%Y%m%d', a.event_date)) >= DATE_ADD(cohort_week_start, INTERVAL 14 DAY)
     AND DATE(PARSE_DATE('%Y%m%d', a.event_date)) < DATE_ADD(cohort_week_start, INTERVAL 21 DAY)
    THEN c.user_pseudo_id 
  END) AS retained_users,
  ROUND(COUNT(DISTINCT CASE 
    WHEN DATE(PARSE_DATE('%Y%m%d', a.event_date)) >= DATE_ADD(cohort_week_start, INTERVAL 14 DAY)
     AND DATE(PARSE_DATE('%Y%m%d', a.event_date)) < DATE_ADD(cohort_week_start, INTERVAL 21 DAY)
    THEN c.user_pseudo_id 
  END) * 100.0 / COUNT(DISTINCT c.user_pseudo_id), 2) AS week2_retention_rate
FROM cohorts c
LEFT JOIN all_sessions a ON c.user_pseudo_id = a.user_pseudo_id
WHERE cohort_week_start BETWEEN DATE '2018-07-09' AND DATE '2018-09-17'
GROUP BY cohort_week_start
ORDER BY cohort_week_start