WITH monthly_nets AS (
  SELECT 
    "customer_id",
    DATE_TRUNC('MONTH', TO_DATE("txn_date", 'YYYY-MM-DD')) AS "month_start",
    SUM(CASE WHEN "txn_type" = 'deposit' THEN "txn_amount" ELSE -"txn_amount" END) AS "net_amount"
  FROM "BANK_SALES_TRADING"."BANK_SALES_TRADING"."CUSTOMER_TRANSACTIONS"
  WHERE "txn_type" IN ('deposit', 'withdrawal')
  GROUP BY "customer_id", "month_start"
),
cumulative_balances AS (
  SELECT 
    "customer_id",
    "month_start",
    SUM("net_amount") OVER (PARTITION BY "customer_id" ORDER BY "month_start" ROWS UNBOUNDED PRECEDING) AS "closing_balance"
  FROM monthly_nets
),
recent_balances AS (
  SELECT 
    "customer_id",
    "closing_balance",
    LAG("closing_balance") OVER (PARTITION BY "customer_id" ORDER BY "month_start") AS "prior_balance"
  FROM cumulative_balances
  QUALIFY ROW_NUMBER() OVER (PARTITION BY "customer_id" ORDER BY "month_start" DESC) = 1
),
growth_rates AS (
  SELECT 
    "customer_id",
    CASE 
      WHEN "prior_balance" = 0 THEN "closing_balance" * 100
      ELSE (("closing_balance" - "prior_balance") / "prior_balance") * 100 
    END AS "growth_rate"
  FROM recent_balances
)
SELECT 
  (COUNT(CASE WHEN "growth_rate" > 5 THEN 1 END) * 100.0 / COUNT(*)) AS "percentage_customers_above_5_percent"
FROM growth_rates