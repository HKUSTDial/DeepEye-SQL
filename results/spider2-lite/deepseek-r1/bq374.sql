WITH all_sessions AS (
  SELECT 
    fullVisitorId,
    `date`,
    totals.newVisits AS newVisits,
    totals.timeOnSite AS timeOnSite,
    totals.transactions AS transactions,
    visitStartTime
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE _TABLE_SUFFIX BETWEEN '20160801' AND '20170731'
),
ranked_new_visits AS (
  SELECT 
    fullVisitorId,
    `date` AS first_date,
    timeOnSite AS first_timeOnSite,
    visitStartTime AS first_visitStartTime,
    ROW_NUMBER() OVER (PARTITION BY fullVisitorId ORDER BY visitStartTime) AS rn
  FROM all_sessions
  WHERE newVisits = 1 
    AND `date` BETWEEN '20160801' AND '20170430'
),
new_users_initial AS (
  SELECT 
    fullVisitorId,
    first_date,
    first_timeOnSite,
    first_visitStartTime
  FROM ranked_new_visits
  WHERE rn = 1
),
new_users_long_first AS (
  SELECT 
    fullVisitorId,
    first_visitStartTime
  FROM new_users_initial
  WHERE first_timeOnSite > 300
),
users_with_subsequent_purchase AS (
  SELECT DISTINCT n.fullVisitorId
  FROM new_users_long_first n
  INNER JOIN all_sessions s 
    ON n.fullVisitorId = s.fullVisitorId 
    AND s.visitStartTime > n.first_visitStartTime
  WHERE s.transactions >= 1
)
SELECT 
  (SELECT COUNT(*) FROM new_users_initial) AS total_new_users,
  (SELECT COUNT(*) FROM users_with_subsequent_purchase) AS users_both_conditions,
  (SELECT COUNT(*) FROM users_with_subsequent_purchase) * 100.0 / (SELECT COUNT(*) FROM new_users_initial) AS percentage