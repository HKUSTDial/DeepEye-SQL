WITH monthly_nets AS (
    SELECT 
        customer_id,
        date(txn_date, 'start of month') AS month_start,
        SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE -txn_amount END) AS net_amount
    FROM customer_transactions
    WHERE txn_type IN ('deposit', 'withdrawal')
    GROUP BY customer_id, month_start
),
closing_balances AS (
    SELECT 
        customer_id,
        month_start,
        net_amount,
        SUM(net_amount) OVER (PARTITION BY customer_id ORDER BY month_start) AS closing_balance
    FROM monthly_nets
),
with_month_sequence AS (
    SELECT 
        customer_id,
        month_start,
        closing_balance,
        LAG(closing_balance) OVER (PARTITION BY customer_id ORDER BY month_start) AS prev_closing_balance,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY month_start DESC) AS rn
    FROM closing_balances
),
latest_info AS (
    SELECT 
        customer_id,
        closing_balance,
        prev_closing_balance
    FROM with_month_sequence
    WHERE rn = 1
),
growth_rates AS (
    SELECT 
        customer_id,
        CASE 
            WHEN prev_closing_balance IS NULL THEN NULL
            WHEN prev_closing_balance = 0 THEN closing_balance * 100
            ELSE (closing_balance - prev_closing_balance) * 100.0 / prev_closing_balance
        END AS growth_rate
    FROM latest_info
)
SELECT 
    (COUNT(CASE WHEN growth_rate > 5 THEN 1 END) * 100.0 / COUNT(*)) AS percentage_customers_high_growth
FROM growth_rates;