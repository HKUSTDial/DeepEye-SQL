WITH all_sessions AS (
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') as session_date,
    "device":"deviceCategory"::TEXT as device_category,
    "totals":"transactions"::INTEGER as transactions
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170512"
  UNION ALL
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') as session_date,
    "device":"deviceCategory"::TEXT as device_category,
    "totals":"transactions"::INTEGER as transactions
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170724"
  UNION ALL
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') as session_date,
    "device":"deviceCategory"::TEXT as device_category,
    "totals":"transactions"::INTEGER as transactions
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20160812"
  UNION ALL
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') as session_date,
    "device":"deviceCategory"::TEXT as device_category,
    "totals":"transactions"::INTEGER as transactions
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20160920"
  UNION ALL
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') as session_date,
    "device":"deviceCategory"::TEXT as device_category,
    "totals":"transactions"::INTEGER as transactions
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20160816"
  UNION ALL
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') as session_date,
    "device":"deviceCategory"::TEXT as device_category,
    "totals":"transactions"::INTEGER as transactions
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170210"
  UNION ALL
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') as session_date,
    "device":"deviceCategory"::TEXT as device_category,
    "totals":"transactions"::INTEGER as transactions
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20161003"
  UNION ALL
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') as session_date,
    "device":"deviceCategory"::TEXT as device_category,
    "totals":"transactions"::INTEGER as transactions
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170521"
  UNION ALL
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') as session_date,
    "device":"deviceCategory"::TEXT as device_category,
    "totals":"transactions"::INTEGER as transactions
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20161204"
  UNION ALL
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') as session_date,
    "device":"deviceCategory"::TEXT as device_category,
    "totals":"transactions"::INTEGER as transactions
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20161127"
),
first_visits AS (
  SELECT 
    "fullVisitorId",
    MIN(session_date) as first_visit_date
  FROM all_sessions
  GROUP BY "fullVisitorId"
),
first_transactions AS (
  SELECT 
    "fullVisitorId",
    MIN(session_date) as first_transaction_date,
    MIN(device_category) as first_transaction_device
  FROM all_sessions
  WHERE transactions > 0
  GROUP BY "fullVisitorId"
)
SELECT 
  fv."fullVisitorId"
FROM first_visits fv
INNER JOIN first_transactions ft ON fv."fullVisitorId" = ft."fullVisitorId"
WHERE ft.first_transaction_date > fv.first_visit_date
  AND ft.first_transaction_device = 'mobile'