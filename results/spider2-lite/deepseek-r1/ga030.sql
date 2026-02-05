WITH first_sessions AS (
  SELECT 
    user_pseudo_id,
    MIN(PARSE_DATE('%Y%m%d', event_date)) AS first_session_date
  FROM `firebase-public-project.analytics_153293282.events_*`
  WHERE event_name = 'session_start'
    AND _TABLE_SUFFIX >= '20180702'
    AND PARSE_DATE('%Y%m%d', event_date) >= '2018-07-02'
  GROUP BY user_pseudo_id
),
cohort_users AS (
  SELECT 
    user_pseudo_id,
    first_session_date,
    DATE_TRUNC(first_session_date, WEEK(MONDAY)) AS cohort_week
  FROM first_sessions
),
returned_week4 AS (
  SELECT DISTINCT
    cu.user_pseudo_id,
    cu.cohort_week
  FROM cohort_users cu
  JOIN `firebase-public-project.analytics_153293282.events_*` e
    ON cu.user_pseudo_id = e.user_pseudo_id
  WHERE e.event_date BETWEEN FORMAT_DATE('%Y%m%d', DATE_ADD(cu.first_session_date, INTERVAL 21 DAY))
                        AND FORMAT_DATE('%Y%m%d', DATE_ADD(cu.first_session_date, INTERVAL 27 DAY))
    AND _TABLE_SUFFIX >= '20180702'
),
cohort_retention AS (
  SELECT 
    c.cohort_week,
    COUNT(DISTINCT c.user_pseudo_id) AS cohort_size,
    COUNT(DISTINCT r.user_pseudo_id) AS retained_users,
    COUNT(DISTINCT r.user_pseudo_id) * 100.0 / COUNT(DISTINCT c.user_pseudo_id) AS retention_rate
  FROM cohort_users c
  LEFT JOIN returned_week4 r 
    ON c.user_pseudo_id = r.user_pseudo_id AND c.cohort_week = r.cohort_week
  GROUP BY c.cohort_week
)
SELECT FORMAT_DATE('%Y-%m-%d', cohort_week) AS cohort_monday
FROM cohort_retention
ORDER BY retention_rate DESC
LIMIT 1