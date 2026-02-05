SELECT driver_id, avg_daily_deliveries
FROM (
    SELECT driver_id, AVG(daily_count) AS avg_daily_deliveries
    FROM (
        SELECT driver_id, delivery_date, COUNT(*) AS daily_count
        FROM (
            SELECT d.driver_id, DATE(o.order_moment_delivered) AS delivery_date
            FROM deliveries d
            INNER JOIN orders o ON d.delivery_order_id = o.delivery_order_id
            WHERE d.delivery_status = 'DELIVERED' AND d.driver_id IS NOT NULL AND o.order_moment_delivered IS NOT NULL
        ) t1
        GROUP BY driver_id, delivery_date
    ) t2
    GROUP BY driver_id
) t3
ORDER BY avg_daily_deliveries DESC
LIMIT 5