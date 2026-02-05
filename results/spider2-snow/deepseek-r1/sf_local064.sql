WITH customer_month_balance AS (
  SELECT 
    "customer_id",
    EXTRACT(MONTH FROM TO_DATE("txn_date", 'YYYY-MM-DD')) AS month_num,
    SUM(CASE WHEN "txn_type" = 'deposit' THEN "txn_amount" ELSE 0 END) -
    SUM(CASE WHEN "txn_type" = 'withdrawal' THEN "txn_amount" ELSE 0 END) AS month_end_balance
  FROM 
    "BANK_SALES_TRADING"."BANK_SALES_TRADING"."CUSTOMER_TRANSACTIONS"
  WHERE 
    EXTRACT(YEAR FROM TO_DATE("txn_date", 'YYYY-MM-DD')) = 2020
  GROUP BY 
    "customer_id", month_num
),
monthly_stats AS (
  SELECT 
    month_num,
    COUNT(CASE WHEN month_end_balance > 0 THEN "customer_id" END) AS positive_customer_count,
    AVG(month_end_balance) AS avg_month_end_balance
  FROM 
    customer_month_balance
  GROUP BY 
    month_num
),
highest_month AS (
  SELECT month_num, avg_month_end_balance
  FROM monthly_stats
  ORDER BY positive_customer_count DESC, month_num ASC
  LIMIT 1
),
lowest_month AS (
  SELECT month_num, avg_month_end_balance
  FROM monthly_stats
  ORDER BY positive_customer_count ASC, month_num ASC
  LIMIT 1
)
SELECT 
  (SELECT avg_month_end_balance FROM highest_month) - 
  (SELECT avg_month_end_balance FROM lowest_month) AS difference
FROM 
  (SELECT 1) AS t