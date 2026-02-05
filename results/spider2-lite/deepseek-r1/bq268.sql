WITH sessions_base AS (
  SELECT 
    fullVisitorId,
    PARSE_DATE('%Y%m%d', date) AS session_date,
    TIMESTAMP_SECONDS(visitStartTime) AS session_timestamp,
    device.deviceCategory AS device_category,
    IFNULL(totals.transactions, 0) AS transactions
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
),
user_first_visit AS (
  SELECT 
    fullVisitorId,
    MIN(session_date) AS first_visit_date
  FROM sessions_base
  GROUP BY fullVisitorId
),
last_visit_session AS (
  SELECT 
    fullVisitorId,
    session_date AS last_visit_date,
    session_timestamp AS last_visit_timestamp,
    device_category AS last_visit_device
  FROM (
    SELECT 
      fullVisitorId,
      session_date,
      session_timestamp,
      device_category,
      ROW_NUMBER() OVER (PARTITION BY fullVisitorId ORDER BY session_date DESC, session_timestamp DESC) AS rn
    FROM sessions_base
  ) 
  WHERE rn = 1
),
first_transaction_session AS (
  SELECT 
    fullVisitorId,
    session_date AS first_transaction_date,
    session_timestamp AS first_transaction_timestamp,
    device_category AS first_transaction_device
  FROM (
    SELECT 
      fullVisitorId,
      session_date,
      session_timestamp,
      device_category,
      ROW_NUMBER() OVER (PARTITION BY fullVisitorId ORDER BY session_date ASC, session_timestamp ASC) AS rn
    FROM sessions_base
    WHERE transactions > 0
  ) 
  WHERE rn = 1
),
combined AS (
  SELECT 
    ufv.fullVisitorId,
    ufv.first_visit_date,
    lvs.last_visit_date,
    lvs.last_visit_timestamp,
    lvs.last_visit_device,
    fts.first_transaction_date,
    fts.first_transaction_timestamp,
    fts.first_transaction_device
  FROM user_first_visit ufv
  LEFT JOIN last_visit_session lvs ON ufv.fullVisitorId = lvs.fullVisitorId
  LEFT JOIN first_transaction_session fts ON ufv.fullVisitorId = fts.fullVisitorId
),
last_event AS (
  SELECT 
    *,
    CASE 
      WHEN first_transaction_timestamp IS NULL THEN last_visit_date
      WHEN last_visit_timestamp >= first_transaction_timestamp THEN last_visit_date
      ELSE first_transaction_date
    END AS last_event_date,
    CASE 
      WHEN first_transaction_timestamp IS NULL THEN last_visit_device
      WHEN last_visit_timestamp >= first_transaction_timestamp THEN last_visit_device
      ELSE first_transaction_device
    END AS last_event_device
  FROM combined
),
filtered_users AS (
  SELECT 
    fullVisitorId,
    first_visit_date,
    last_event_date,
    DATE_DIFF(last_event_date, first_visit_date, DAY) AS days_diff
  FROM last_event
  WHERE last_event_device = 'mobile'
)
SELECT MAX(days_diff) AS longest_days FROM filtered_users