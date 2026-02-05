WITH driver_season_participations AS (
    SELECT 
        r.`driver_id`,
        ra.`year`,
        ra.`round`,
        r.`constructor_id`,
        ROW_NUMBER() OVER (PARTITION BY r.`driver_id`, ra.`year` ORDER BY ra.`round`) AS rn_asc,
        ROW_NUMBER() OVER (PARTITION BY r.`driver_id`, ra.`year` ORDER BY ra.`round` DESC) AS rn_desc
    FROM `results` r
    INNER JOIN `races` ra ON r.`race_id` = ra.`race_id`
    WHERE ra.`year` BETWEEN 1950 AND 1959
),
driver_season_aggregates AS (
    SELECT 
        `driver_id`,
        `year`,
        MAX(CASE WHEN rn_asc = 1 THEN `constructor_id` END) AS first_constructor,
        MAX(CASE WHEN rn_desc = 1 THEN `constructor_id` END) AS last_constructor,
        COUNT(DISTINCT `round`) AS distinct_rounds
    FROM driver_season_participations
    GROUP BY `driver_id`, `year`
    HAVING COUNT(DISTINCT `round`) >= 2 
       AND first_constructor = last_constructor
)
SELECT DISTINCT 
    d.`driver_id`,
    d.`forename`,
    d.`surname`
FROM driver_season_aggregates ds
INNER JOIN `drivers` d ON ds.`driver_id` = d.`driver_id`
ORDER BY d.`surname`, d.`forename`