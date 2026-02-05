WITH feb_sessions AS (
  SELECT
    fullVisitorId,
    PARSE_DATE('%Y%m%d', date) AS session_date,
    device.deviceCategory AS device_category,
    visitStartTime,
    IFNULL(totals.transactions, 0) AS transactions
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201702*`
  WHERE _TABLE_SUFFIX BETWEEN '01' AND '28'
),
first_visit AS (
  SELECT
    fullVisitorId,
    MIN(session_date) AS first_visit_date
  FROM feb_sessions
  GROUP BY fullVisitorId
),
first_transaction_session AS (
  SELECT
    fullVisitorId,
    session_date AS first_transaction_date,
    device_category,
    visitStartTime,
    ROW_NUMBER() OVER (PARTITION BY fullVisitorId ORDER BY session_date, visitStartTime) AS rn
  FROM feb_sessions
  WHERE transactions > 0
)
SELECT
  fv.fullVisitorId,
  fv.first_visit_date,
  fts.first_transaction_date,
  DATE_DIFF(fts.first_transaction_date, fv.first_visit_date, DAY) AS days_elapsed,
  fts.device_category
FROM first_visit fv
INNER JOIN first_transaction_session fts ON fv.fullVisitorId = fts.fullVisitorId AND fts.rn = 1
ORDER BY fv.fullVisitorId