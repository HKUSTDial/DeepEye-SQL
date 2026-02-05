WITH overall_range AS (
  SELECT 
    MIN(TO_DATE("txn_date")) AS overall_min_date,
    MAX(TO_DATE("txn_date")) AS overall_max_date
  FROM "BANK_SALES_TRADING"."BANK_SALES_TRADING"."CUSTOMER_TRANSACTIONS"
),
numbers_0_9 AS (
  SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
),
numbers AS (
  SELECT (a.n * 1000 + b.n * 100 + c.n * 10 + d.n) AS n
  FROM numbers_0_9 a
  CROSS JOIN numbers_0_9 b
  CROSS JOIN numbers_0_9 c
  CROSS JOIN numbers_0_9 d
),
all_dates_series AS (
  SELECT DATEADD(day, n, o.overall_min_date) AS generated_date
  FROM numbers
  CROSS JOIN overall_range o
  WHERE n <= DATEDIFF(day, o.overall_min_date, o.overall_max_date)
),
customer_ranges AS (
  SELECT 
    "customer_id",
    MIN(TO_DATE("txn_date")) AS min_date,
    MAX(TO_DATE("txn_date")) AS max_date
  FROM "BANK_SALES_TRADING"."BANK_SALES_TRADING"."CUSTOMER_TRANSACTIONS"
  GROUP BY "customer_id"
),
all_dates AS (
  SELECT 
    cr."customer_id",
    ads.generated_date AS "date"
  FROM customer_ranges cr
  CROSS JOIN all_dates_series ads
  WHERE ads.generated_date BETWEEN cr.min_date AND cr.max_date
),
daily_net AS (
  SELECT 
    TO_DATE("txn_date") AS "txn_date",
    "customer_id",
    SUM(CASE WHEN "txn_type" = 'deposit' THEN "txn_amount" ELSE -"txn_amount" END) AS daily_net
  FROM "BANK_SALES_TRADING"."BANK_SALES_TRADING"."CUSTOMER_TRANSACTIONS"
  GROUP BY TO_DATE("txn_date"), "customer_id"
),
daily_data AS (
  SELECT 
    ad."customer_id",
    ad."date",
    COALESCE(dn.daily_net, 0) AS daily_net
  FROM all_dates ad
  LEFT JOIN daily_net dn 
    ON ad."customer_id" = dn."customer_id" 
    AND ad."date" = dn."txn_date"
),
running_sums AS (
  SELECT 
    "customer_id",
    "date",
    daily_net,
    SUM(daily_net) OVER (PARTITION BY "customer_id" ORDER BY "date" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum
  FROM daily_data
),
min_running_sums AS (
  SELECT 
    "customer_id",
    "date",
    daily_net,
    running_sum,
    MIN(running_sum) OVER (PARTITION BY "customer_id" ORDER BY "date" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_running_sum
  FROM running_sums
),
daily_balances AS (
  SELECT 
    "customer_id",
    "date",
    running_sum + GREATEST(0, -min_running_sum) AS balance
  FROM min_running_sums
),
monthly_max_balances AS (
  SELECT 
    "customer_id",
    DATE_TRUNC('month', "date") AS month_start,
    MAX(balance) AS max_daily_balance
  FROM daily_balances
  GROUP BY "customer_id", DATE_TRUNC('month', "date")
)
SELECT 
  TO_CHAR(month_start, 'YYYY-MM') AS month,
  SUM(max_daily_balance) AS monthly_total_max_balances
FROM monthly_max_balances
GROUP BY month_start
ORDER BY month_start