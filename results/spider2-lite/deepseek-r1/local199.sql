WITH rental_counts AS (
    SELECT 
        s.`store_id`,
        strftime('%Y', r.`rental_date`) as `year`,
        strftime('%m', r.`rental_date`) as `month`,
        COUNT(*) as `rental_count`
    FROM `rental` r
    JOIN `staff` s ON r.`staff_id` = s.`staff_id`
    GROUP BY s.`store_id`, `year`, `month`
),
max_counts AS (
    SELECT 
        `store_id`,
        MAX(`rental_count`) as `max_count`
    FROM rental_counts
    GROUP BY `store_id`
)
SELECT 
    rc.`store_id`,
    rc.`year`,
    rc.`month`,
    rc.`rental_count`
FROM rental_counts rc
JOIN max_counts mc ON rc.`store_id` = mc.`store_id` AND rc.`rental_count` = mc.`max_count`
ORDER BY rc.`store_id`, rc.`year`, rc.`month`