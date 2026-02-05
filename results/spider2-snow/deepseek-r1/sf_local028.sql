WITH months AS (
  SELECT ROW_NUMBER() OVER (ORDER BY NULL) AS month
  FROM TABLE(GENERATOR(ROWCOUNT=>12))
), order_data AS (
  SELECT 
    EXTRACT(YEAR FROM TO_TIMESTAMP("order_purchase_timestamp", 'YYYY-MM-DD HH24:MI:SS')) AS year,
    EXTRACT(MONTH FROM TO_TIMESTAMP("order_purchase_timestamp", 'YYYY-MM-DD HH24:MI:SS')) AS month,
    COUNT(*) AS cnt
  FROM "BRAZILIAN_E_COMMERCE"."BRAZILIAN_E_COMMERCE"."OLIST_ORDERS"
  WHERE "order_status" = 'delivered'
    AND EXTRACT(YEAR FROM TO_TIMESTAMP("order_purchase_timestamp", 'YYYY-MM-DD HH24:MI:SS')) IN (2016, 2017, 2018)
  GROUP BY year, month
)
SELECT 
  months.month,
  COALESCE(SUM(CASE WHEN order_data.year = 2016 THEN order_data.cnt END), 0) AS "2016",
  COALESCE(SUM(CASE WHEN order_data.year = 2017 THEN order_data.cnt END), 0) AS "2017",
  COALESCE(SUM(CASE WHEN order_data.year = 2018 THEN order_data.cnt END), 0) AS "2018"
FROM months
LEFT JOIN order_data ON months.month = order_data.month
GROUP BY months.month
ORDER BY months.month