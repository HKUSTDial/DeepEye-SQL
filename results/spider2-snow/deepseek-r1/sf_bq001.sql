WITH all_feb_sessions AS (
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') AS session_date,
    "totals":"transactions"::INTEGER AS transactions,
    "device":"deviceCategory"::TEXT AS device_category,
    "visitStartTime"
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170201"
  WHERE "date" LIKE '201702%'
  
  UNION ALL
  
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') AS session_date,
    "totals":"transactions"::INTEGER AS transactions,
    "device":"deviceCategory"::TEXT AS device_category,
    "visitStartTime"
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170202"
  WHERE "date" LIKE '201702%'
  
  UNION ALL
  
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') AS session_date,
    "totals":"transactions"::INTEGER AS transactions,
    "device":"deviceCategory"::TEXT AS device_category,
    "visitStartTime"
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170203"
  WHERE "date" LIKE '201702%'
  
  UNION ALL
  
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') AS session_date,
    "totals":"transactions"::INTEGER AS transactions,
    "device":"deviceCategory"::TEXT AS device_category,
    "visitStartTime"
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170204"
  WHERE "date" LIKE '201702%'
  
  UNION ALL
  
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') AS session_date,
    "totals":"transactions"::INTEGER AS transactions,
    "device":"deviceCategory"::TEXT AS device_category,
    "visitStartTime"
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170205"
  WHERE "date" LIKE '201702%'
  
  UNION ALL
  
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') AS session_date,
    "totals":"transactions"::INTEGER AS transactions,
    "device":"deviceCategory"::TEXT AS device_category,
    "visitStartTime"
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170206"
  WHERE "date" LIKE '201702%'
  
  UNION ALL
  
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') AS session_date,
    "totals":"transactions"::INTEGER AS transactions,
    "device":"deviceCategory"::TEXT AS device_category,
    "visitStartTime"
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170207"
  WHERE "date" LIKE '201702%'
  
  UNION ALL
  
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') AS session_date,
    "totals":"transactions"::INTEGER AS transactions,
    "device":"deviceCategory"::TEXT AS device_category,
    "visitStartTime"
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170208"
  WHERE "date" LIKE '201702%'
  
  UNION ALL
  
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') AS session_date,
    "totals":"transactions"::INTEGER AS transactions,
    "device":"deviceCategory"::TEXT AS device_category,
    "visitStartTime"
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170209"
  WHERE "date" LIKE '201702%'
  
  UNION ALL
  
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') AS session_date,
    "totals":"transactions"::INTEGER AS transactions,
    "device":"deviceCategory"::TEXT AS device_category,
    "visitStartTime"
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170210"
  WHERE "date" LIKE '201702%'
  
  UNION ALL
  
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') AS session_date,
    "totals":"transactions"::INTEGER AS transactions,
    "device":"deviceCategory"::TEXT AS device_category,
    "visitStartTime"
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170211"
  WHERE "date" LIKE '201702%'
  
  UNION ALL
  
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') AS session_date,
    "totals":"transactions"::INTEGER AS transactions,
    "device":"deviceCategory"::TEXT AS device_category,
    "visitStartTime"
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170212"
  WHERE "date" LIKE '201702%'
  
  UNION ALL
  
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') AS session_date,
    "totals":"transactions"::INTEGER AS transactions,
    "device":"deviceCategory"::TEXT AS device_category,
    "visitStartTime"
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170213"
  WHERE "date" LIKE '201702%'
  
  UNION ALL
  
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') AS session_date,
    "totals":"transactions"::INTEGER AS transactions,
    "device":"deviceCategory"::TEXT AS device_category,
    "visitStartTime"
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170214"
  WHERE "date" LIKE '201702%'
  
  UNION ALL
  
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') AS session_date,
    "totals":"transactions"::INTEGER AS transactions,
    "device":"deviceCategory"::TEXT AS device_category,
    "visitStartTime"
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170215"
  WHERE "date" LIKE '201702%'
  
  UNION ALL
  
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') AS session_date,
    "totals":"transactions"::INTEGER AS transactions,
    "device":"deviceCategory"::TEXT AS device_category,
    "visitStartTime"
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170216"
  WHERE "date" LIKE '201702%'
  
  UNION ALL
  
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') AS session_date,
    "totals":"transactions"::INTEGER AS transactions,
    "device":"deviceCategory"::TEXT AS device_category,
    "visitStartTime"
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170217"
  WHERE "date" LIKE '201702%'
  
  UNION ALL
  
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') AS session_date,
    "totals":"transactions"::INTEGER AS transactions,
    "device":"deviceCategory"::TEXT AS device_category,
    "visitStartTime"
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170218"
  WHERE "date" LIKE '201702%'
  
  UNION ALL
  
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') AS session_date,
    "totals":"transactions"::INTEGER AS transactions,
    "device":"deviceCategory"::TEXT AS device_category,
    "visitStartTime"
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170219"
  WHERE "date" LIKE '201702%'
  
  UNION ALL
  
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') AS session_date,
    "totals":"transactions"::INTEGER AS transactions,
    "device":"deviceCategory"::TEXT AS device_category,
    "visitStartTime"
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170220"
  WHERE "date" LIKE '201702%'
  
  UNION ALL
  
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') AS session_date,
    "totals":"transactions"::INTEGER AS transactions,
    "device":"deviceCategory"::TEXT AS device_category,
    "visitStartTime"
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170221"
  WHERE "date" LIKE '201702%'
  
  UNION ALL
  
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') AS session_date,
    "totals":"transactions"::INTEGER AS transactions,
    "device":"deviceCategory"::TEXT AS device_category,
    "visitStartTime"
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170222"
  WHERE "date" LIKE '201702%'
  
  UNION ALL
  
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') AS session_date,
    "totals":"transactions"::INTEGER AS transactions,
    "device":"deviceCategory"::TEXT AS device_category,
    "visitStartTime"
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170223"
  WHERE "date" LIKE '201702%'
  
  UNION ALL
  
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') AS session_date,
    "totals":"transactions"::INTEGER AS transactions,
    "device":"deviceCategory"::TEXT AS device_category,
    "visitStartTime"
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170224"
  WHERE "date" LIKE '201702%'
  
  UNION ALL
  
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') AS session_date,
    "totals":"transactions"::INTEGER AS transactions,
    "device":"deviceCategory"::TEXT AS device_category,
    "visitStartTime"
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170225"
  WHERE "date" LIKE '201702%'
  
  UNION ALL
  
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') AS session_date,
    "totals":"transactions"::INTEGER AS transactions,
    "device":"deviceCategory"::TEXT AS device_category,
    "visitStartTime"
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170226"
  WHERE "date" LIKE '201702%'
  
  UNION ALL
  
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') AS session_date,
    "totals":"transactions"::INTEGER AS transactions,
    "device":"deviceCategory"::TEXT AS device_category,
    "visitStartTime"
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170227"
  WHERE "date" LIKE '201702%'
  
  UNION ALL
  
  SELECT 
    "fullVisitorId",
    TO_DATE("date", 'YYYYMMDD') AS session_date,
    "totals":"transactions"::INTEGER AS transactions,
    "device":"deviceCategory"::TEXT AS device_category,
    "visitStartTime"
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170228"
  WHERE "date" LIKE '201702%'
),
visitors_with_transactions AS (
  SELECT DISTINCT "fullVisitorId"
  FROM all_feb_sessions
  WHERE COALESCE(transactions, 0) >= 1
),
first_visit AS (
  SELECT 
    "fullVisitorId",
    MIN(session_date) AS first_visit_date
  FROM all_feb_sessions
  GROUP BY "fullVisitorId"
),
ranked_transactions AS (
  SELECT 
    "fullVisitorId",
    session_date AS first_transaction_date,
    device_category,
    ROW_NUMBER() OVER (PARTITION BY "fullVisitorId" ORDER BY session_date, "visitStartTime") AS rn
  FROM all_feb_sessions
  WHERE COALESCE(transactions, 0) >= 1
),
first_transaction AS (
  SELECT 
    "fullVisitorId",
    first_transaction_date,
    device_category
  FROM ranked_transactions
  WHERE rn = 1
)
SELECT 
  vwt."fullVisitorId",
  DATEDIFF(day, fv.first_visit_date, ft.first_transaction_date) AS days_elapsed,
  ft.device_category AS device_type
FROM visitors_with_transactions vwt
JOIN first_visit fv ON vwt."fullVisitorId" = fv."fullVisitorId"
JOIN first_transaction ft ON vwt."fullVisitorId" = ft."fullVisitorId"
ORDER BY vwt."fullVisitorId"