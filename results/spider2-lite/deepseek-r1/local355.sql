WITH `driver_intervals` AS (
  SELECT 
    `driver_id`,
    `year`,
    `constructor_id`,
    `first_round`,
    `last_round`,
    LEAD(`first_round`) OVER (PARTITION BY `driver_id`, `year` ORDER BY `first_round`) AS `next_first_round`,
    LEAD(`constructor_id`) OVER (PARTITION BY `driver_id`, `year` ORDER BY `first_round`) AS `next_constructor_id`
  FROM `drives`
),
`gaps` AS (
  SELECT
    `driver_id`,
    `year`,
    `constructor_id` AS `constructor_before`,
    `next_constructor_id` AS `constructor_after`,
    `last_round` + 1 AS `first_missed_round`,
    `next_first_round` - 1 AS `last_missed_round`,
    (`next_first_round` - `last_round` - 1) AS `missed_count`
  FROM `driver_intervals`
  WHERE `next_first_round` IS NOT NULL 
    AND `next_first_round` > `last_round` + 1
),
`races_per_year` AS (
  SELECT `year`, COUNT(*) AS `total_races`
  FROM `races`
  GROUP BY `year`
),
`driver_year_participated` AS (
  SELECT `driver_id`, `year`, SUM(`last_round` - `first_round` + 1) AS `participated_rounds`
  FROM `drives`
  GROUP BY `driver_id`, `year`
),
`driver_year_total_missed` AS (
  SELECT 
    p.`driver_id`,
    p.`year`,
    r.`total_races` - p.`participated_rounds` AS `total_missed`
  FROM `driver_year_participated` p
  JOIN `races_per_year` r ON p.`year` = r.`year`
),
`filtered_gaps` AS (
  SELECT 
    g.`first_missed_round`,
    g.`last_missed_round`
  FROM `gaps` g
  INNER JOIN `driver_year_total_missed` t 
    ON g.`driver_id` = t.`driver_id` AND g.`year` = t.`year`
  WHERE t.`total_missed` < 3
    AND g.`constructor_before` != g.`constructor_after`
)
SELECT 
  AVG(`first_missed_round`) AS `overall_avg_first_round`,
  AVG(`last_missed_round`) AS `overall_avg_last_round`
FROM `filtered_gaps`;