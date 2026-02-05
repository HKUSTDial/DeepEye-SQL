WITH months(month) AS (
    SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10 UNION SELECT 11 UNION SELECT 12
),
yearly_data AS (
    SELECT 
        CAST(strftime('%Y', `order_delivered_customer_date`) AS INTEGER) AS year,
        CAST(strftime('%m', `order_delivered_customer_date`) AS INTEGER) AS month,
        COUNT(*) AS order_count
    FROM `olist_orders`
    WHERE `order_status` = 'delivered'
        AND `order_delivered_customer_date` IS NOT NULL
        AND strftime('%Y', `order_delivered_customer_date`) IN ('2016','2017','2018')
    GROUP BY year, month
)
SELECT 
    months.month,
    COALESCE(SUM(CASE WHEN yearly_data.year = 2016 THEN yearly_data.order_count END), 0) AS "2016",
    COALESCE(SUM(CASE WHEN yearly_data.year = 2017 THEN yearly_data.order_count END), 0) AS "2017",
    COALESCE(SUM(CASE WHEN yearly_data.year = 2018 THEN yearly_data.order_count END), 0) AS "2018"
FROM months
LEFT JOIN yearly_data ON months.month = yearly_data.month
GROUP BY months.month
ORDER BY months.month;