SELECT h.hub_id, h.hub_name,
       counts.feb_count,
       counts.mar_count,
       (counts.mar_count - counts.feb_count) * 1.0 / counts.feb_count AS increase_pct
FROM (
    SELECT s.hub_id,
           SUM(CASE WHEN o.order_created_month = 2 THEN 1 ELSE 0 END) AS feb_count,
           SUM(CASE WHEN o.order_created_month = 3 THEN 1 ELSE 0 END) AS mar_count
    FROM orders o
    JOIN stores s ON o.store_id = s.store_id
    WHERE o.order_status = 'FINISHED'
      AND o.order_created_year = 2021
      AND o.order_created_month IN (2, 3)
    GROUP BY s.hub_id
) counts
JOIN hubs h ON counts.hub_id = h.hub_id
WHERE counts.feb_count > 0
  AND (counts.mar_count - counts.feb_count) * 1.0 / counts.feb_count > 0.2
ORDER BY h.hub_id;