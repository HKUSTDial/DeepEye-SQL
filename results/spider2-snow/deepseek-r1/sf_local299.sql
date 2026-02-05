WITH RECURSIVE all_dates AS (
  SELECT 
    "customer_id",
    min_date AS dt,
    max_date
  FROM (
    SELECT
      "customer_id",
      MIN(TO_DATE("txn_date")) AS min_date,
      MAX(TO_DATE("txn_date")) AS max_date
    FROM "BANK_SALES_TRADING"."BANK_SALES_TRADING"."CUSTOMER_TRANSACTIONS"
    GROUP BY "customer_id"
  )
  UNION ALL
  SELECT
    "customer_id",
    DATEADD(day, 1, dt),
    max_date
  FROM all_dates
  WHERE dt < max_date
),
daily_net AS (
  SELECT
    "customer_id",
    TO_DATE("txn_date") AS txn_date,
    SUM(CASE WHEN "txn_type" = 'deposit' THEN "txn_amount" ELSE -"txn_amount" END) AS net_change
  FROM "BANK_SALES_TRADING"."BANK_SALES_TRADING"."CUSTOMER_TRANSACTIONS"
  GROUP BY "customer_id", TO_DATE("txn_date")
),
daily_balances AS (
  SELECT
    ad."customer_id",
    ad.dt,
    COALESCE(dn.net_change, 0) AS net_change,
    SUM(COALESCE(dn.net_change, 0)) OVER (PARTITION BY ad."customer_id" ORDER BY ad.dt ROWS UNBOUNDED PRECEDING) AS closing_balance
  FROM all_dates ad
  LEFT JOIN daily_net dn ON ad."customer_id" = dn."customer_id" AND ad.dt = dn.txn_date
),
rolling_avg_data AS (
  SELECT
    "customer_id",
    dt,
    closing_balance,
    AVG(closing_balance) OVER (PARTITION BY "customer_id" ORDER BY dt ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_avg_raw,
    ROW_NUMBER() OVER (PARTITION BY "customer_id" ORDER BY dt) AS day_seq
  FROM daily_balances
),
rolling_avg_adjusted AS (
  SELECT
    "customer_id",
    dt,
    CASE
      WHEN day_seq >= 30 AND rolling_avg_raw < 0 THEN 0
      WHEN day_seq >= 30 THEN rolling_avg_raw
      ELSE NULL
    END AS rolling_avg_balance
  FROM rolling_avg_data
),
first_month AS (
  SELECT
    "customer_id",
    DATE_TRUNC('month', MIN(TO_DATE("txn_date"))) AS first_month
  FROM "BANK_SALES_TRADING"."BANK_SALES_TRADING"."CUSTOMER_TRANSACTIONS"
  GROUP BY "customer_id"
),
monthly_max AS (
  SELECT
    ra."customer_id",
    DATE_TRUNC('month', ra.dt) AS month,
    MAX(ra.rolling_avg_balance) AS max_rolling_avg
  FROM rolling_avg_adjusted ra
  GROUP BY ra."customer_id", DATE_TRUNC('month', ra.dt)
)
SELECT
  mm.month,
  SUM(mm.max_rolling_avg) AS total_sum_max_rolling_avg
FROM monthly_max mm
LEFT JOIN first_month fm ON mm."customer_id" = fm."customer_id"
WHERE mm.month != fm.first_month
GROUP BY mm.month
ORDER BY mm.month;