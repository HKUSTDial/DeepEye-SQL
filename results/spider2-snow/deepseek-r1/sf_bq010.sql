WITH july_purchases AS (
  SELECT 
    s."fullVisitorId",
    product.value:"v2ProductName" AS productName,
    CAST(product.value:"productQuantity" AS INTEGER) AS productQuantity
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170701" s,
  LATERAL FLATTEN(INPUT => s."hits") hit,
  LATERAL FLATTEN(INPUT => hit.value:"product") product
  WHERE hit.value:"eCommerceAction"."action_type" = '6'
  UNION ALL
  SELECT 
    s."fullVisitorId",
    product.value:"v2ProductName" AS productName,
    CAST(product.value:"productQuantity" AS INTEGER) AS productQuantity
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170702" s,
  LATERAL FLATTEN(INPUT => s."hits") hit,
  LATERAL FLATTEN(INPUT => hit.value:"product") product
  WHERE hit.value:"eCommerceAction"."action_type" = '6'
  UNION ALL
  SELECT 
    s."fullVisitorId",
    product.value:"v2ProductName" AS productName,
    CAST(product.value:"productQuantity" AS INTEGER) AS productQuantity
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170703" s,
  LATERAL FLATTEN(INPUT => s."hits") hit,
  LATERAL FLATTEN(INPUT => hit.value:"product") product
  WHERE hit.value:"eCommerceAction"."action_type" = '6'
  UNION ALL
  SELECT 
    s."fullVisitorId",
    product.value:"v2ProductName" AS productName,
    CAST(product.value:"productQuantity" AS INTEGER) AS productQuantity
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170704" s,
  LATERAL FLATTEN(INPUT => s."hits") hit,
  LATERAL FLATTEN(INPUT => hit.value:"product") product
  WHERE hit.value:"eCommerceAction"."action_type" = '6'
  UNION ALL
  SELECT 
    s."fullVisitorId",
    product.value:"v2ProductName" AS productName,
    CAST(product.value:"productQuantity" AS INTEGER) AS productQuantity
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170705" s,
  LATERAL FLATTEN(INPUT => s."hits") hit,
  LATERAL FLATTEN(INPUT => hit.value:"product") product
  WHERE hit.value:"eCommerceAction"."action_type" = '6'
  UNION ALL
  SELECT 
    s."fullVisitorId",
    product.value:"v2ProductName" AS productName,
    CAST(product.value:"productQuantity" AS INTEGER) AS productQuantity
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170706" s,
  LATERAL FLATTEN(INPUT => s."hits") hit,
  LATERAL FLATTEN(INPUT => hit.value:"product") product
  WHERE hit.value:"eCommerceAction"."action_type" = '6'
  UNION ALL
  SELECT 
    s."fullVisitorId",
    product.value:"v2ProductName" AS productName,
    CAST(product.value:"productQuantity" AS INTEGER) AS productQuantity
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170707" s,
  LATERAL FLATTEN(INPUT => s."hits") hit,
  LATERAL FLATTEN(INPUT => hit.value:"product") product
  WHERE hit.value:"eCommerceAction"."action_type" = '6'
  UNION ALL
  SELECT 
    s."fullVisitorId",
    product.value:"v2ProductName" AS productName,
    CAST(product.value:"productQuantity" AS INTEGER) AS productQuantity
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170708" s,
  LATERAL FLATTEN(INPUT => s."hits") hit,
  LATERAL FLATTEN(INPUT => hit.value:"product") product
  WHERE hit.value:"eCommerceAction"."action_type" = '6'
  UNION ALL
  SELECT 
    s."fullVisitorId",
    product.value:"v2ProductName" AS productName,
    CAST(product.value:"productQuantity" AS INTEGER) AS productQuantity
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170709" s,
  LATERAL FLATTEN(INPUT => s."hits") hit,
  LATERAL FLATTEN(INPUT => hit.value:"product") product
  WHERE hit.value:"eCommerceAction"."action_type" = '6'
  UNION ALL
  SELECT 
    s."fullVisitorId",
    product.value:"v2ProductName" AS productName,
    CAST(product.value:"productQuantity" AS INTEGER) AS productQuantity
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170710" s,
  LATERAL FLATTEN(INPUT => s."hits") hit,
  LATERAL FLATTEN(INPUT => hit.value:"product") product
  WHERE hit.value:"eCommerceAction"."action_type" = '6'
  UNION ALL
  SELECT 
    s."fullVisitorId",
    product.value:"v2ProductName" AS productName,
    CAST(product.value:"productQuantity" AS INTEGER) AS productQuantity
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170711" s,
  LATERAL FLATTEN(INPUT => s."hits") hit,
  LATERAL FLATTEN(INPUT => hit.value:"product") product
  WHERE hit.value:"eCommerceAction"."action_type" = '6'
  UNION ALL
  SELECT 
    s."fullVisitorId",
    product.value:"v2ProductName" AS productName,
    CAST(product.value:"productQuantity" AS INTEGER) AS productQuantity
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170712" s,
  LATERAL FLATTEN(INPUT => s."hits") hit,
  LATERAL FLATTEN(INPUT => hit.value:"product") product
  WHERE hit.value:"eCommerceAction"."action_type" = '6'
  UNION ALL
  SELECT 
    s."fullVisitorId",
    product.value:"v2ProductName" AS productName,
    CAST(product.value:"productQuantity" AS INTEGER) AS productQuantity
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170713" s,
  LATERAL FLATTEN(INPUT => s."hits") hit,
  LATERAL FLATTEN(INPUT => hit.value:"product") product
  WHERE hit.value:"eCommerceAction"."action_type" = '6'
  UNION ALL
  SELECT 
    s."fullVisitorId",
    product.value:"v2ProductName" AS productName,
    CAST(product.value:"productQuantity" AS INTEGER) AS productQuantity
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170714" s,
  LATERAL FLATTEN(INPUT => s."hits") hit,
  LATERAL FLATTEN(INPUT => hit.value:"product") product
  WHERE hit.value:"eCommerceAction"."action_type" = '6'
  UNION ALL
  SELECT 
    s."fullVisitorId",
    product.value:"v2ProductName" AS productName,
    CAST(product.value:"productQuantity" AS INTEGER) AS productQuantity
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170715" s,
  LATERAL FLATTEN(INPUT => s."hits") hit,
  LATERAL FLATTEN(INPUT => hit.value:"product") product
  WHERE hit.value:"eCommerceAction"."action_type" = '6'
  UNION ALL
  SELECT 
    s."fullVisitorId",
    product.value:"v2ProductName" AS productName,
    CAST(product.value:"productQuantity" AS INTEGER) AS productQuantity
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170716" s,
  LATERAL FLATTEN(INPUT => s."hits") hit,
  LATERAL FLATTEN(INPUT => hit.value:"product") product
  WHERE hit.value:"eCommerceAction"."action_type" = '6'
  UNION ALL
  SELECT 
    s."fullVisitorId",
    product.value:"v2ProductName" AS productName,
    CAST(product.value:"productQuantity" AS INTEGER) AS productQuantity
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170717" s,
  LATERAL FLATTEN(INPUT => s."hits") hit,
  LATERAL FLATTEN(INPUT => hit.value:"product") product
  WHERE hit.value:"eCommerceAction"."action_type" = '6'
  UNION ALL
  SELECT 
    s."fullVisitorId",
    product.value:"v2ProductName" AS productName,
    CAST(product.value:"productQuantity" AS INTEGER) AS productQuantity
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170718" s,
  LATERAL FLATTEN(INPUT => s."hits") hit,
  LATERAL FLATTEN(INPUT => hit.value:"product") product
  WHERE hit.value:"eCommerceAction"."action_type" = '6'
  UNION ALL
  SELECT 
    s."fullVisitorId",
    product.value:"v2ProductName" AS productName,
    CAST(product.value:"productQuantity" AS INTEGER) AS productQuantity
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170719" s,
  LATERAL FLATTEN(INPUT => s."hits") hit,
  LATERAL FLATTEN(INPUT => hit.value:"product") product
  WHERE hit.value:"eCommerceAction"."action_type" = '6'
  UNION ALL
  SELECT 
    s."fullVisitorId",
    product.value:"v2ProductName" AS productName,
    CAST(product.value:"productQuantity" AS INTEGER) AS productQuantity
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170720" s,
  LATERAL FLATTEN(INPUT => s."hits") hit,
  LATERAL FLATTEN(INPUT => hit.value:"product") product
  WHERE hit.value:"eCommerceAction"."action_type" = '6'
  UNION ALL
  SELECT 
    s."fullVisitorId",
    product.value:"v2ProductName" AS productName,
    CAST(product.value:"productQuantity" AS INTEGER) AS productQuantity
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170721" s,
  LATERAL FLATTEN(INPUT => s."hits") hit,
  LATERAL FLATTEN(INPUT => hit.value:"product") product
  WHERE hit.value:"eCommerceAction"."action_type" = '6'
  UNION ALL
  SELECT 
    s."fullVisitorId",
    product.value:"v2ProductName" AS productName,
    CAST(product.value:"productQuantity" AS INTEGER) AS productQuantity
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170722" s,
  LATERAL FLATTEN(INPUT => s."hits") hit,
  LATERAL FLATTEN(INPUT => hit.value:"product") product
  WHERE hit.value:"eCommerceAction"."action_type" = '6'
  UNION ALL
  SELECT 
    s."fullVisitorId",
    product.value:"v2ProductName" AS productName,
    CAST(product.value:"productQuantity" AS INTEGER) AS productQuantity
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170723" s,
  LATERAL FLATTEN(INPUT => s."hits") hit,
  LATERAL FLATTEN(INPUT => hit.value:"product") product
  WHERE hit.value:"eCommerceAction"."action_type" = '6'
  UNION ALL
  SELECT 
    s."fullVisitorId",
    product.value:"v2ProductName" AS productName,
    CAST(product.value:"productQuantity" AS INTEGER) AS productQuantity
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170724" s,
  LATERAL FLATTEN(INPUT => s."hits") hit,
  LATERAL FLATTEN(INPUT => hit.value:"product") product
  WHERE hit.value:"eCommerceAction"."action_type" = '6'
  UNION ALL
  SELECT 
    s."fullVisitorId",
    product.value:"v2ProductName" AS productName,
    CAST(product.value:"productQuantity" AS INTEGER) AS productQuantity
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170725" s,
  LATERAL FLATTEN(INPUT => s."hits") hit,
  LATERAL FLATTEN(INPUT => hit.value:"product") product
  WHERE hit.value:"eCommerceAction"."action_type" = '6'
  UNION ALL
  SELECT 
    s."fullVisitorId",
    product.value:"v2ProductName" AS productName,
    CAST(product.value:"productQuantity" AS INTEGER) AS productQuantity
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170726" s,
  LATERAL FLATTEN(INPUT => s."hits") hit,
  LATERAL FLATTEN(INPUT => hit.value:"product") product
  WHERE hit.value:"eCommerceAction"."action_type" = '6'
  UNION ALL
  SELECT 
    s."fullVisitorId",
    product.value:"v2ProductName" AS productName,
    CAST(product.value:"productQuantity" AS INTEGER) AS productQuantity
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170727" s,
  LATERAL FLATTEN(INPUT => s."hits") hit,
  LATERAL FLATTEN(INPUT => hit.value:"product") product
  WHERE hit.value:"eCommerceAction"."action_type" = '6'
  UNION ALL
  SELECT 
    s."fullVisitorId",
    product.value:"v2ProductName" AS productName,
    CAST(product.value:"productQuantity" AS INTEGER) AS productQuantity
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170728" s,
  LATERAL FLATTEN(INPUT => s."hits") hit,
  LATERAL FLATTEN(INPUT => hit.value:"product") product
  WHERE hit.value:"eCommerceAction"."action_type" = '6'
  UNION ALL
  SELECT 
    s."fullVisitorId",
    product.value:"v2ProductName" AS productName,
    CAST(product.value:"productQuantity" AS INTEGER) AS productQuantity
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170729" s,
  LATERAL FLATTEN(INPUT => s."hits") hit,
  LATERAL FLATTEN(INPUT => hit.value:"product") product
  WHERE hit.value:"eCommerceAction"."action_type" = '6'
  UNION ALL
  SELECT 
    s."fullVisitorId",
    product.value:"v2ProductName" AS productName,
    CAST(product.value:"productQuantity" AS INTEGER) AS productQuantity
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170730" s,
  LATERAL FLATTEN(INPUT => s."hits") hit,
  LATERAL FLATTEN(INPUT => hit.value:"product") product
  WHERE hit.value:"eCommerceAction"."action_type" = '6'
  UNION ALL
  SELECT 
    s."fullVisitorId",
    product.value:"v2ProductName" AS productName,
    CAST(product.value:"productQuantity" AS INTEGER) AS productQuantity
  FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170731" s,
  LATERAL FLATTEN(INPUT => s."hits") hit,
  LATERAL FLATTEN(INPUT => hit.value:"product") product
  WHERE hit.value:"eCommerceAction"."action_type" = '6'
),
target_customers AS (
  SELECT DISTINCT "fullVisitorId"
  FROM july_purchases
  WHERE productName = 'Youtube Men’s Vintage Henley'
),
other_purchases AS (
  SELECT 
    jp."fullVisitorId",
    jp.productName,
    jp.productQuantity
  FROM july_purchases jp
  INNER JOIN target_customers tc ON jp."fullVisitorId" = tc."fullVisitorId"
  WHERE jp.productName != 'Youtube Men’s Vintage Henley'
),
product_totals AS (
  SELECT 
    productName,
    SUM(productQuantity) AS total_quantity
  FROM other_purchases
  GROUP BY productName
  ORDER BY total_quantity DESC
)
SELECT productName, total_quantity
FROM product_totals
LIMIT 1