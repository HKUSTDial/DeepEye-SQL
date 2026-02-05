WITH italian_customers AS (
    SELECT cust_id 
    FROM customers c
    JOIN countries co ON c.country_id = co.country_id
    WHERE co.country_name = 'Italy'
),
dec_2021_sales AS (
    SELECT s.cust_id, s.quantity_sold, s.prod_id, s.channel_id, s.time_id, s.promo_id
    FROM sales s
    JOIN times t ON s.time_id = t.time_id
    WHERE t.calendar_year = 2021 AND t.calendar_month_number = 12
),
profit_calc AS (
    SELECT ds.cust_id, 
           ds.quantity_sold * (c.unit_price - c.unit_cost) as sale_profit
    FROM dec_2021_sales ds
    JOIN costs c ON ds.prod_id = c.prod_id 
                 AND ds.channel_id = c.channel_id
                 AND ds.time_id = c.time_id
                 AND ds.promo_id = c.promo_id
),
customer_totals AS (
    SELECT ic.cust_id, SUM(pc.sale_profit) as total_profit
    FROM italian_customers ic
    JOIN profit_calc pc ON ic.cust_id = pc.cust_id
    GROUP BY ic.cust_id
),
profit_range AS (
    SELECT MIN(total_profit) as min_profit,
           MAX(total_profit) as max_profit,
           (MAX(total_profit) - MIN(total_profit)) / 10.0 as bucket_width
    FROM customer_totals
)
SELECT 
    CASE 
        WHEN ct.total_profit = pr.max_profit THEN 10
        ELSE CAST((ct.total_profit - pr.min_profit) / pr.bucket_width AS INTEGER) + 1
    END as bucket,
    COUNT(DISTINCT ct.cust_id) as customer_count,
    MIN(ct.total_profit) as min_profit_in_bucket,
    MAX(ct.total_profit) as max_profit_in_bucket
FROM customer_totals ct
CROSS JOIN profit_range pr
GROUP BY bucket
ORDER BY bucket