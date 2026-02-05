WITH all_2017_sessions AS (
  SELECT 
    "trafficSource":"source"::STRING AS traffic_source,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC AS transaction_revenue
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170101"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170102"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170103"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170104"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170105"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170106"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170107"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170108"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170109"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170110"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170111"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170112"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170113"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170114"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170115"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170116"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170117"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170118"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170119"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170120"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170121"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170122"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170123"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170124"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170125"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170126"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170127"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170128"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170129"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170130"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170131"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170201"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170202"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170203"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170204"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170205"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170206"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170207"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170208"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170209"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170210"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170211"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170212"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170213"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170214"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170215"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170216"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170217"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170218"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170219"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170220"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170221"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170222"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170223"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170224"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170225"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170226"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170227"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170228"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170301"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170302"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170303"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170304"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170305"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170306"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170307"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170308"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170309"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170310"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170311"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170312"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170313"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170314"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170315"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170316"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170317"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170318"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170319"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170320"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170321"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170322"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170323"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170324"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170325"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170326"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170327"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170328"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170329"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170330"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170331"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170401"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170402"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170403"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170404"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170405"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170406"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170407"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170408"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170409"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170410"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170411"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170412"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170413"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170414"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170415"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170416"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170417"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170418"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170419"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170420"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170421"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170422"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170423"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170424"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170425"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170426"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170427"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170428"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170429"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170430"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170501"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170502"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170503"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170504"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170505"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170506"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170507"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170508"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170509"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170510"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170511"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170512"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170513"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170514"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170515"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170516"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170517"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170518"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170519"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170520"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170521"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170522"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170523"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170524"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170525"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170526"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170527"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170528"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170529"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170530"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170531"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170601"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170602"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170603"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170604"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170605"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170606"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170607"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170608"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170609"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170610"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170611"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170612"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170613"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170614"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170615"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170616"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170617"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170618"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170619"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170620"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170621"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170622"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170623"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170624"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170625"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170626"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170627"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170628"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170629"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170630"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170701"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170702"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170703"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170704"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170705"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170706"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170707"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170708"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170709"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170710"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170711"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170712"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170713"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170714"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170715"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170716"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170717"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170718"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170719"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170720"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170721"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170722"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170723"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170724"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170725"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170726"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170727"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170728"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170729"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170730"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170731"
  WHERE SUBSTR("date", 1, 4) = '2017'
  UNION ALL
  SELECT 
    "trafficSource":"source"::STRING,
    "date",
    "totals":"totalTransactionRevenue"::NUMERIC
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170801"
  WHERE SUBSTR("date", 1, 4) = '2017'
),
yearly_totals AS (
  SELECT 
    traffic_source,
    SUM(transaction_revenue) AS yearly_revenue
  FROM all_2017_sessions
  WHERE transaction_revenue IS NOT NULL
  GROUP BY traffic_source
),
top_traffic_source AS (
  SELECT traffic_source
  FROM yearly_totals
  ORDER BY yearly_revenue DESC
  LIMIT 1
),
monthly_totals AS (
  SELECT 
    SUBSTR("date", 5, 2) AS month,
    SUM(transaction_revenue) AS monthly_revenue
  FROM all_2017_sessions
  WHERE transaction_revenue IS NOT NULL
    AND traffic_source = (SELECT traffic_source FROM top_traffic_source)
  GROUP BY month
)
SELECT 
  (SELECT traffic_source FROM top_traffic_source) AS top_traffic_source,
  ROUND((MAX(monthly_revenue) - MIN(monthly_revenue)) / 1000000, 2) AS revenue_diff_millions
FROM monthly_totals