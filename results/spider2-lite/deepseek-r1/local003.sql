WITH delivered_orders AS (
    SELECT 
        c.customer_unique_id,
        o.order_id,
        o.order_purchase_timestamp,
        op.payment_value
    FROM `orders` o
    JOIN `customers` c ON o.customer_id = c.customer_id
    JOIN `order_payments` op ON o.order_id = op.order_id
    WHERE o.order_status = 'delivered'
),
customer_metrics AS (
    SELECT 
        customer_unique_id,
        MAX(order_purchase_timestamp) as last_purchase_date,
        COUNT(DISTINCT order_id) as order_count,
        SUM(payment_value) as total_spend
    FROM delivered_orders
    GROUP BY customer_unique_id
),
with_days_ago AS (
    SELECT 
        *,
        JULIANDAY((SELECT MAX(order_purchase_timestamp) FROM `orders` WHERE order_status = 'delivered')) - JULIANDAY(last_purchase_date) as days_ago
    FROM customer_metrics
),
ranked_metrics AS (
    SELECT 
        customer_unique_id,
        last_purchase_date,
        order_count,
        total_spend,
        days_ago,
        ROW_NUMBER() OVER (ORDER BY days_ago ASC) as rn_recency,
        COUNT(*) OVER () as total_customers,
        ROW_NUMBER() OVER (ORDER BY order_count DESC) as rn_frequency,
        ROW_NUMBER() OVER (ORDER BY total_spend DESC) as rn_monetary
    FROM with_days_ago
),
scored AS (
    SELECT 
        customer_unique_id,
        last_purchase_date,
        order_count,
        total_spend,
        days_ago,
        CASE 
            WHEN rn_recency <= total_customers * 0.2 THEN 1
            WHEN rn_recency <= total_customers * 0.4 THEN 2
            WHEN rn_recency <= total_customers * 0.6 THEN 3
            WHEN rn_recency <= total_customers * 0.8 THEN 4
            ELSE 5
        END as recency_score,
        CASE 
            WHEN rn_frequency <= total_customers * 0.2 THEN 1
            WHEN rn_frequency <= total_customers * 0.4 THEN 2
            WHEN rn_frequency <= total_customers * 0.6 THEN 3
            WHEN rn_frequency <= total_customers * 0.8 THEN 4
            ELSE 5
        END as frequency_score,
        CASE 
            WHEN rn_monetary <= total_customers * 0.2 THEN 1
            WHEN rn_monetary <= total_customers * 0.4 THEN 2
            WHEN rn_monetary <= total_customers * 0.6 THEN 3
            WHEN rn_monetary <= total_customers * 0.8 THEN 4
            ELSE 5
        END as monetary_score
    FROM ranked_metrics
),
segmented AS (
    SELECT 
        customer_unique_id,
        order_count,
        total_spend,
        recency_score,
        frequency_score,
        monetary_score,
        frequency_score + monetary_score as fm_sum,
        CASE 
            WHEN recency_score = 1 AND (frequency_score + monetary_score) BETWEEN 1 AND 4 THEN 'Champions'
            WHEN recency_score IN (4,5) AND (frequency_score + monetary_score) BETWEEN 1 AND 2 THEN 'Cant Lose Them'
            WHEN recency_score IN (4,5) AND (frequency_score + monetary_score) BETWEEN 3 AND 6 THEN 'Hibernating'
            WHEN recency_score IN (4,5) AND (frequency_score + monetary_score) BETWEEN 7 AND 10 THEN 'Lost'
            WHEN recency_score IN (2,3) AND (frequency_score + monetary_score) BETWEEN 1 AND 4 THEN 'Loyal Customers'
            WHEN recency_score = 3 AND (frequency_score + monetary_score) BETWEEN 5 AND 6 THEN 'Needs Attention'
            WHEN recency_score = 1 AND (frequency_score + monetary_score) BETWEEN 7 AND 8 THEN 'Recent Users'
            WHEN (recency_score = 1 AND (frequency_score + monetary_score) BETWEEN 5 AND 6) 
                 OR (recency_score = 2 AND (frequency_score + monetary_score) BETWEEN 5 AND 8) THEN 'Potential Loyalists'
            WHEN recency_score = 1 AND (frequency_score + monetary_score) BETWEEN 9 AND 10 THEN 'Price Sensitive'
            WHEN recency_score = 2 AND (frequency_score + monetary_score) BETWEEN 9 AND 10 THEN 'Promising'
            WHEN recency_score = 3 AND (frequency_score + monetary_score) BETWEEN 7 AND 10 THEN 'About to Sleep'
            ELSE 'Other'
        END as rfm_segment
    FROM scored
)
SELECT 
    rfm_segment,
    COUNT(DISTINCT customer_unique_id) as customer_count,
    AVG(total_spend / order_count) as avg_sales_per_order,
    MIN(total_spend / order_count) as min_avg_sales,
    MAX(total_spend / order_count) as max_avg_sales
FROM segmented
GROUP BY rfm_segment
ORDER BY avg_sales_per_order DESC;