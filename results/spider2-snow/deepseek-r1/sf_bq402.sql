SELECT 
  COUNT(DISTINCT CASE WHEN "totals":"transactions" IS NOT NULL THEN "fullVisitorId" END) * 1.0 / NULLIF(COUNT(DISTINCT "fullVisitorId"), 0) AS conversion_rate,
  SUM("totals":"transactions"::NUMBER) * 1.0 / NULLIF(COUNT(DISTINCT CASE WHEN "totals":"transactions" IS NOT NULL THEN "fullVisitorId" END), 0) AS avg_transactions_per_purchaser
FROM "ECOMMERCE"."ECOMMERCE"."WEB_ANALYTICS"