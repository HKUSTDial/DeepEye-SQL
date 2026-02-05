WITH customer_monthly_balances AS (
    SELECT 
        customer_id,
        strftime('%Y-%m', txn_date) AS month,
        SUM(CASE 
            WHEN txn_type = 'deposit' THEN txn_amount 
            WHEN txn_type = 'withdrawal' THEN -txn_amount 
            ELSE 0 
        END) AS month_end_balance
    FROM customer_transactions
    WHERE strftime('%Y', txn_date) = '2020'
        AND txn_type IN ('deposit', 'withdrawal')
    GROUP BY customer_id, month
),
monthly_positive_counts AS (
    SELECT 
        month,
        COUNT(*) AS positive_customers_count
    FROM customer_monthly_balances
    WHERE month_end_balance > 0
    GROUP BY month
),
max_min_months AS (
    SELECT 
        (SELECT month FROM monthly_positive_counts ORDER BY positive_customers_count DESC LIMIT 1) AS max_month,
        (SELECT month FROM monthly_positive_counts ORDER BY positive_customers_count ASC LIMIT 1) AS min_month
),
averages AS (
    SELECT 
        (SELECT AVG(month_end_balance) FROM customer_monthly_balances WHERE month = (SELECT max_month FROM max_min_months)) AS avg_max_month,
        (SELECT AVG(month_end_balance) FROM customer_monthly_balances WHERE month = (SELECT min_month FROM max_min_months)) AS avg_min_month
)
SELECT 
    avg_max_month - avg_min_month AS difference
FROM averages;