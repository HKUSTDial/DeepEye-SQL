WITH races_with_pit AS (
    SELECT `race_id` FROM `races_ext` WHERE `is_pit_data_available` = 1
),
lap_data AS (
    SELECT lp.`race_id`, lp.`driver_id`, lp.`lap`, lp.`position`
    FROM `lap_positions` lp
    INNER JOIN races_with_pit r ON lp.`race_id` = r.race_id
),
driver_laps AS (
    SELECT `race_id`, `driver_id`, `lap`, `position`,
           LAG(`position`) OVER (PARTITION BY `race_id`, `driver_id` ORDER BY `lap`) as prev_position
    FROM lap_data
),
overtakes AS (
    SELECT 
        A.`race_id`,
        A.`lap`,
        A.`driver_id` as overtaken_driver_id,
        B.`driver_id` as overtaking_driver_id
    FROM driver_laps A
    INNER JOIN driver_laps B 
        ON A.`race_id` = B.`race_id` 
        AND A.`lap` = B.`lap`
        AND A.`driver_id` != B.`driver_id`
    WHERE A.prev_position IS NOT NULL 
      AND B.prev_position IS NOT NULL
      AND A.prev_position < B.prev_position
      AND A.`position` > B.`position`
),
retirement_check AS (
    SELECT o.*,
           CASE WHEN ret.`driver_id` IS NOT NULL THEN 1 ELSE 0 END as is_retirement
    FROM overtakes o
    LEFT JOIN `retirements` ret 
        ON o.race_id = ret.`race_id` 
        AND o.overtaken_driver_id = ret.`driver_id` 
        AND ret.`lap` = o.lap
),
pit_check AS (
    SELECT rc.*,
           CASE WHEN EXISTS (SELECT 1 FROM `pit_stops` ps 
                             WHERE ps.`race_id` = rc.race_id 
                               AND ps.`driver_id` = rc.overtaken_driver_id 
                               AND ps.`lap` IN (rc.lap, rc.lap - 1)) 
                THEN 1 ELSE 0 END as is_pit_related
    FROM retirement_check rc
),
grid_data AS (
    SELECT pc.*,
           r1.`grid` as grid_overtaken,
           r2.`grid` as grid_overtaking
    FROM pit_check pc
    LEFT JOIN `results` r1 ON pc.race_id = r1.`race_id` AND pc.overtaken_driver_id = r1.`driver_id`
    LEFT JOIN `results` r2 ON pc.race_id = r2.`race_id` AND pc.overtaking_driver_id = r2.`driver_id`
),
classified AS (
    SELECT *,
        CASE 
            WHEN is_retirement = 1 THEN 'R'
            WHEN is_pit_related = 1 THEN 'P'
            WHEN lap = 1 AND grid_overtaken IS NOT NULL AND grid_overtaking IS NOT NULL 
                 AND ABS(grid_overtaken - grid_overtaking) <= 1 THEN 'S'
            ELSE 'T'
        END as overtake_type
    FROM grid_data
)
SELECT overtake_type, COUNT(*) as count
FROM classified
GROUP BY overtake_type
ORDER BY overtake_type;