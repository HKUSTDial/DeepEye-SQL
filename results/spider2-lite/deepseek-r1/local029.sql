WITH order_payments AS (
    SELECT order_id, SUM(payment_value) AS total_payment
    FROM olist_order_payments
    GROUP BY order_id
)
SELECT 
    c.customer_unique_id,
    AVG(op.total_payment) AS avg_payment_value,
    MIN(c.customer_city) AS customer_city,
    MIN(c.customer_state) AS customer_state
FROM olist_orders o
JOIN olist_customers c ON o.customer_id = c.customer_id
LEFT JOIN order_payments op ON o.order_id = op.order_id
WHERE o.order_status = 'delivered'
GROUP BY c.customer_unique_id
ORDER BY COUNT(o.order_id) DESC
LIMIT 3;