SELECT t.employeeid, COALESCE(l.late_orders, 0) AS late_orders, (COALESCE(l.late_orders, 0) * 100.0 / t.total_orders) AS late_percentage
FROM (SELECT employeeid, COUNT(*) AS total_orders FROM orders GROUP BY employeeid) t
LEFT JOIN (SELECT employeeid, COUNT(*) AS late_orders FROM orders WHERE shippeddate IS NOT NULL AND shippeddate >= requireddate GROUP BY employeeid) l
ON t.employeeid = l.employeeid
WHERE t.total_orders > 50
ORDER BY late_percentage DESC
LIMIT 3