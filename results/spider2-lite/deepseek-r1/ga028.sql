WITH cohort AS (
  SELECT user_pseudo_id
  FROM `firebase-public-project.analytics_153293282.events_*`
  WHERE event_name = 'session_start'
    AND _TABLE_SUFFIX BETWEEN '20180612' AND '20181002'
    AND event_date BETWEEN '20180612' AND '20181002'
  GROUP BY user_pseudo_id
  HAVING MIN(event_date) BETWEEN '20180702' AND '20180708'
)
SELECT 
  COUNT(DISTINCT c.user_pseudo_id) AS week_0_new_users,
  COUNT(DISTINCT CASE WHEN e.event_date BETWEEN '20180709' AND '20180715' THEN c.user_pseudo_id END) AS week_1_retained,
  COUNT(DISTINCT CASE WHEN e.event_date BETWEEN '20180716' AND '20180722' THEN c.user_pseudo_id END) AS week_2_retained,
  COUNT(DISTINCT CASE WHEN e.event_date BETWEEN '20180723' AND '20180729' THEN c.user_pseudo_id END) AS week_3_retained,
  COUNT(DISTINCT CASE WHEN e.event_date BETWEEN '20180730' AND '20180805' THEN c.user_pseudo_id END) AS week_4_retained
FROM cohort c
LEFT JOIN `firebase-public-project.analytics_153293282.events_*` e 
  ON c.user_pseudo_id = e.user_pseudo_id
  AND e._TABLE_SUFFIX BETWEEN '20180612' AND '20181002'
  AND e.event_date BETWEEN '20180709' AND '20180805'