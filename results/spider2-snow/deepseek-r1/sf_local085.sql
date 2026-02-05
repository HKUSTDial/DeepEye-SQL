WITH employee_stats AS (
    SELECT 
        "employeeid",
        COUNT(*) AS total_orders,
        SUM(CASE WHEN TRY_TO_DATE("shippeddate") >= TRY_TO_DATE("requireddate") THEN 1 ELSE 0 END) AS late_orders
    FROM "NORTHWIND"."NORTHWIND"."ORDERS"
    WHERE "shippeddate" IS NOT NULL AND TRIM("shippeddate") != ''
    GROUP BY "employeeid"
    HAVING total_orders > 50
)
SELECT 
    "employeeid",
    late_orders,
    (late_orders * 100.0 / total_orders) AS late_order_percentage
FROM employee_stats
ORDER BY late_order_percentage DESC
LIMIT 3