WITH customer_agg AS (
    SELECT 
        c.customer_unique_id,
        COUNT(DISTINCT o.order_id) AS num_orders,
        SUM(p.payment_value) AS total_payment,
        MIN(o.order_purchase_timestamp) AS first_purchase,
        MAX(o.order_purchase_timestamp) AS last_purchase
    FROM `orders` o
    JOIN `customers` c ON o.customer_id = c.customer_id
    JOIN `order_payments` p ON o.order_id = p.order_id
    GROUP BY c.customer_unique_id
),
customer_metrics AS (
    SELECT 
        customer_unique_id,
        num_orders,
        total_payment / num_orders AS avg_payment_per_order,
        CASE 
            WHEN (JULIANDAY(last_purchase) - JULIANDAY(first_purchase)) / 7.0 < 1.0 
            THEN 1.0 
            ELSE (JULIANDAY(last_purchase) - JULIANDAY(first_purchase)) / 7.0 
        END AS customer_lifespan_weeks
    FROM customer_agg
)
SELECT 
    customer_unique_id,
    num_orders,
    avg_payment_per_order,
    customer_lifespan_weeks
FROM customer_metrics
ORDER BY avg_payment_per_order DESC
LIMIT 3