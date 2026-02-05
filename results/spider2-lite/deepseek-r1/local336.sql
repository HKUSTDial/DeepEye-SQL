WITH driver_laps AS (
    SELECT 
        `race_id`,
        `driver_id`,
        `lap`,
        `position` AS curr_position,
        LAG(`position`, 1) OVER (PARTITION BY `race_id`, `driver_id` ORDER BY `lap`) AS prev_position,
        MAX(CASE WHEN `lap` = 0 THEN `position` END) OVER (PARTITION BY `race_id`, `driver_id`) AS grid_position
    FROM `lap_positions`
    WHERE `lap` BETWEEN 0 AND 5
),
overtakes_base AS (
    SELECT 
        a.`race_id`,
        a.`lap` AS overtake_lap,
        a.`driver_id` AS overtaking_driver,
        b.`driver_id` AS overtaken_driver,
        a.grid_position AS a_grid,
        b.grid_position AS b_grid
    FROM driver_laps a
    JOIN driver_laps b ON a.`race_id` = b.`race_id` AND a.`lap` = b.`lap`
    WHERE a.`lap` BETWEEN 1 AND 5
        AND a.`driver_id` != b.`driver_id`
        AND a.prev_position > b.prev_position
        AND a.curr_position < b.curr_position
),
overtakes_classified AS (
    SELECT 
        ob.*,
        CASE 
            WHEN r.`driver_id` IS NOT NULL THEN 'R'
            WHEN p1.`driver_id` IS NOT NULL OR p2.`driver_id` IS NOT NULL THEN 'P'
            WHEN ob.overtake_lap = 1 AND ABS(ob.a_grid - ob.b_grid) <= 2 THEN 'S'
            ELSE 'T'
        END AS category
    FROM overtakes_base ob
    LEFT JOIN `retirements` r ON r.`race_id` = ob.`race_id` AND r.`driver_id` = ob.overtaken_driver AND CAST(r.`lap` AS INTEGER) = ob.overtake_lap
    LEFT JOIN `pit_stops` p1 ON p1.`race_id` = ob.`race_id` AND p1.`driver_id` = ob.overtaken_driver AND p1.`lap` = ob.overtake_lap
    LEFT JOIN `pit_stops` p2 ON p2.`race_id` = ob.`race_id` AND p2.`driver_id` = ob.overtaken_driver AND p2.`lap` = ob.overtake_lap - 1
),
categories AS (
    SELECT 'R' AS category UNION ALL SELECT 'P' UNION ALL SELECT 'S' UNION ALL SELECT 'T'
)
SELECT c.category, COALESCE(COUNT(oc.category), 0) AS overtake_count
FROM categories c
LEFT JOIN overtakes_classified oc ON c.category = oc.category
GROUP BY c.category
ORDER BY c.category;