WITH RECURSIVE dates(date) AS (
    SELECT DATE(MIN(`txn_date`), 'start of month') FROM `customer_transactions`
    UNION ALL
    SELECT DATE(date, '+1 month')
    FROM dates
    WHERE DATE(date, '+1 month') <= (SELECT DATE(MAX(`txn_date`), 'start of month') FROM `customer_transactions`)
),
months AS (
    SELECT strftime('%Y-%m', date) AS month FROM dates
),
customers AS (
    SELECT DISTINCT `customer_id` FROM `customer_transactions`
),
calendar AS (
    SELECT `customer_id`, month
    FROM customers
    CROSS JOIN months
),
monthly_changes AS (
    SELECT `customer_id`, 
           strftime('%Y-%m', `txn_date`) AS month,
           SUM(CASE WHEN `txn_type` = 'deposit' THEN `txn_amount` ELSE -`txn_amount` END) AS monthly_change
    FROM `customer_transactions`
    GROUP BY `customer_id`, month
),
joined AS (
    SELECT c.`customer_id`, c.month, 
           COALESCE(m.monthly_change, 0) AS monthly_change
    FROM calendar c
    LEFT JOIN monthly_changes m ON c.`customer_id` = m.`customer_id` AND c.month = m.month
)
SELECT `customer_id`, month, monthly_change,
       SUM(monthly_change) OVER (PARTITION BY `customer_id` ORDER BY month) AS cumulative_balance
FROM joined
ORDER BY `customer_id`, month