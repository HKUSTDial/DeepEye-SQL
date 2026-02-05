WITH RECURSIVE months(month_start) AS (
    SELECT date(min(txn_date), 'start of month') 
    FROM customer_transactions
    UNION ALL
    SELECT date(month_start, '+1 month')
    FROM months
    WHERE date(month_start, '+1 month') <= (SELECT date(max(txn_date), 'start of month') FROM customer_transactions)
),
transaction_net AS (
    SELECT 
        customer_id,
        txn_date,
        CASE 
            WHEN txn_type = 'deposit' THEN txn_amount 
            ELSE -txn_amount 
        END AS net
    FROM customer_transactions
)
SELECT 
    strftime('%Y-%m', m.month_start) AS month,
    COALESCE((
        SELECT SUM(
            CASE 
                WHEN cust_balance < 0 THEN 0 
                ELSE cust_balance 
            END
        )
        FROM (
            SELECT 
                customer_id,
                SUM(net) AS cust_balance
            FROM transaction_net tn
            WHERE tn.txn_date < m.month_start
            GROUP BY customer_id
        ) cust
    ), 0) AS total_balance
FROM months m
WHERE m.month_start > (SELECT date(min(txn_date), 'start of month') FROM customer_transactions)
ORDER BY m.month_start ASC;