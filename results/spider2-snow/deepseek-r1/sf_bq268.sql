WITH all_sessions AS (
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') AS session_date,
    "device":"deviceCategory"::STRING AS device_category,
    COALESCE("totals":"transactions"::INTEGER, 0) AS transactions
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170512"
  UNION ALL
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') AS session_date,
    "device":"deviceCategory"::STRING AS device_category,
    COALESCE("totals":"transactions"::INTEGER, 0) AS transactions
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170724"
  UNION ALL
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') AS session_date,
    "device":"deviceCategory"::STRING AS device_category,
    COALESCE("totals":"transactions"::INTEGER, 0) AS transactions
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20160812"
),
user_first_visit AS (
  SELECT 
    "fullVisitorId",
    MIN(session_date) AS first_visit_date
  FROM all_sessions
  GROUP BY "fullVisitorId"
),
last_visit_details AS (
  SELECT 
    "fullVisitorId",
    MAX(session_date) AS last_visit_date,
    ARRAY_AGG(device_category) WITHIN GROUP (ORDER BY session_date DESC)[0]::STRING AS last_visit_device
  FROM all_sessions
  GROUP BY "fullVisitorId"
),
first_transaction_details AS (
  SELECT 
    "fullVisitorId",
    MIN(session_date) AS first_transaction_date,
    ARRAY_AGG(device_category) WITHIN GROUP (ORDER BY session_date ASC)[0]::STRING AS first_transaction_device
  FROM all_sessions
  WHERE transactions > 0
  GROUP BY "fullVisitorId"
),
user_events AS (
  SELECT 
    fv."fullVisitorId",
    fv.first_visit_date,
    lv.last_visit_date,
    lv.last_visit_device,
    ft.first_transaction_date,
    ft.first_transaction_device,
    CASE 
      WHEN lv.last_visit_date >= COALESCE(ft.first_transaction_date, '1900-01-01') 
        THEN lv.last_visit_date
      ELSE ft.first_transaction_date
    END AS last_recorded_event_date,
    CASE 
      WHEN lv.last_visit_date >= COALESCE(ft.first_transaction_date, '1900-01-01') 
        THEN lv.last_visit_device
      ELSE ft.first_transaction_device
    END AS last_recorded_event_device
  FROM user_first_visit fv
  LEFT JOIN last_visit_details lv ON fv."fullVisitorId" = lv."fullVisitorId"
  LEFT JOIN first_transaction_details ft ON fv."fullVisitorId" = ft."fullVisitorId"
),
mobile_users AS (
  SELECT 
    "fullVisitorId",
    first_visit_date,
    last_recorded_event_date,
    DATEDIFF(day, first_visit_date, last_recorded_event_date) AS days_diff
  FROM user_events
  WHERE last_recorded_event_device = 'mobile'
)
SELECT MAX(days_diff) AS longest_days
FROM mobile_users