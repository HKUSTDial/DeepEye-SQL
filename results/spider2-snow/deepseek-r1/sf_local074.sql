WITH date_range AS (
  SELECT
    DATE_TRUNC('month', MIN(TO_DATE("txn_date", 'YYYY-MM-DD'))) AS min_month,
    DATE_TRUNC('month', MAX(TO_DATE("txn_date", 'YYYY-MM-DD'))) AS max_month
  FROM "BANK_SALES_TRADING"."BANK_SALES_TRADING"."CUSTOMER_TRANSACTIONS"
),
months AS (
  SELECT min_month AS month_date
  FROM date_range
  UNION ALL
  SELECT DATEADD('month', 1, month_date)
  FROM months
  WHERE month_date < (SELECT max_month FROM date_range)
),
distinct_customers AS (
  SELECT DISTINCT "customer_id"
  FROM "BANK_SALES_TRADING"."BANK_SALES_TRADING"."CUSTOMER_NODES"
),
all_combinations AS (
  SELECT dc."customer_id", m.month_date
  FROM distinct_customers dc
  CROSS JOIN months m
),
monthly_changes AS (
  SELECT
    "customer_id",
    DATE_TRUNC('month', TO_DATE("txn_date", 'YYYY-MM-DD')) AS month_date,
    SUM(CASE WHEN "txn_type" = 'deposit' THEN "txn_amount" ELSE -"txn_amount" END) AS monthly_change
  FROM "BANK_SALES_TRADING"."BANK_SALES_TRADING"."CUSTOMER_TRANSACTIONS"
  GROUP BY "customer_id", month_date
),
with_changes AS (
  SELECT
    ac."customer_id",
    ac.month_date,
    COALESCE(mc.monthly_change, 0) AS monthly_change
  FROM all_combinations ac
  LEFT JOIN monthly_changes mc
    ON ac."customer_id" = mc."customer_id" AND ac.month_date = mc.month_date
),
cumulative_balances AS (
  SELECT
    "customer_id",
    month_date,
    monthly_change,
    SUM(monthly_change) OVER (PARTITION BY "customer_id" ORDER BY month_date ROWS UNBOUNDED PRECEDING) AS closing_balance
  FROM with_changes
)
SELECT
  "customer_id",
  TO_VARCHAR(month_date, 'YYYY-MM') AS month,
  monthly_change,
  closing_balance
FROM cumulative_balances
ORDER BY "customer_id", month_date