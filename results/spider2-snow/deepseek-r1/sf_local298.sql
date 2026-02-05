WITH transaction_dates AS (
  SELECT "customer_id", TO_DATE("txn_date", 'YYYY-MM-DD') AS txn_date, "txn_amount"
  FROM "BANK_SALES_TRADING"."BANK_SALES_TRADING"."CUSTOMER_TRANSACTIONS"
),
daily_balances AS (
  SELECT "customer_id", txn_date,
    SUM("txn_amount") OVER (PARTITION BY "customer_id" ORDER BY txn_date) AS balance
  FROM transaction_dates
),
customers AS (
  SELECT DISTINCT "customer_id"
  FROM transaction_dates
),
months AS (
  SELECT DISTINCT DATE_TRUNC('MONTH', txn_date) AS month_start
  FROM transaction_dates
),
customer_months AS (
  SELECT c."customer_id", m.month_start
  FROM customers c
  CROSS JOIN months m
),
customer_month_balances AS (
  SELECT cm.month_start, cm."customer_id", db.balance,
    ROW_NUMBER() OVER (PARTITION BY cm.month_start, cm."customer_id" ORDER BY db.txn_date DESC) AS rn
  FROM customer_months cm
  LEFT JOIN daily_balances db ON db."customer_id" = cm."customer_id" AND db.txn_date < cm.month_start
),
latest_balances AS (
  SELECT month_start, "customer_id", COALESCE(balance, 0) AS balance
  FROM customer_month_balances
  WHERE rn = 1
),
monthly_totals AS (
  SELECT month_start,
    SUM(GREATEST(balance, 0)) AS total_balance
  FROM latest_balances
  GROUP BY month_start
)
SELECT month_start, total_balance
FROM monthly_totals
WHERE month_start > (SELECT MIN(month_start) FROM monthly_totals)
ORDER BY month_start