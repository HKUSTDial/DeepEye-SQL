WITH RECURSIVE customer_ranges AS (
    SELECT customer_id, MIN(txn_date) AS min_date, MAX(txn_date) AS max_date
    FROM customer_transactions
    GROUP BY customer_id
),
date_series AS (
    SELECT customer_id, min_date AS date, max_date
    FROM customer_ranges
    UNION ALL
    SELECT customer_id, DATE(date, '+1 day'), max_date
    FROM date_series
    WHERE date < max_date
),
daily_transactions AS (
    SELECT customer_id, txn_date,
           SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE -txn_amount END) AS daily_net
    FROM customer_transactions
    GROUP BY customer_id, txn_date
),
daily_net AS (
    SELECT ds.customer_id, ds.date, COALESCE(dt.daily_net, 0) AS daily_net
    FROM date_series ds
    LEFT JOIN daily_transactions dt ON ds.customer_id = dt.customer_id AND ds.date = dt.txn_date
),
ordered_dates AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY date) AS rn
    FROM daily_net
),
balance_recursive AS (
    SELECT customer_id, date, daily_net,
           CASE WHEN daily_net < 0 THEN 0 ELSE daily_net END AS balance,
           rn
    FROM ordered_dates
    WHERE rn = 1
    UNION ALL
    SELECT od.customer_id, od.date, od.daily_net,
           CASE WHEN br.balance + od.daily_net < 0 THEN 0 ELSE br.balance + od.daily_net END AS balance,
           od.rn
    FROM ordered_dates od
    JOIN balance_recursive br ON od.customer_id = br.customer_id AND od.rn = br.rn + 1
),
monthly_max_per_customer AS (
    SELECT customer_id,
           strftime('%Y-%m', date) AS month,
           MAX(balance) AS max_balance
    FROM balance_recursive
    GROUP BY customer_id, strftime('%Y-%m', date)
)
SELECT month, SUM(max_balance) AS monthly_total
FROM monthly_max_per_customer
GROUP BY month
ORDER BY month;