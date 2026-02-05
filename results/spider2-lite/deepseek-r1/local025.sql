WITH `batsman_agg` AS (
    SELECT `match_id`, `innings_no`, `over_id`, SUM(`runs_scored`) AS `batsman_runs`
    FROM `batsman_scored`
    GROUP BY `match_id`, `innings_no`, `over_id`
),
`extra_agg` AS (
    SELECT `match_id`, `innings_no`, `over_id`, SUM(`extra_runs`) AS `extra_runs_sum`
    FROM `extra_runs`
    GROUP BY `match_id`, `innings_no`, `over_id`
),
`over_totals` AS (
    SELECT 
        b.`match_id`,
        b.`innings_no`,
        b.`over_id`,
        b.`batsman_runs` + COALESCE(e.`extra_runs_sum`, 0) AS `total_runs`
    FROM `batsman_agg` b
    LEFT JOIN `extra_agg` e 
        ON b.`match_id` = e.`match_id` 
        AND b.`innings_no` = e.`innings_no` 
        AND b.`over_id` = e.`over_id`
),
`ranked_overs` AS (
    SELECT 
        `match_id`,
        `innings_no`,
        `over_id`,
        `total_runs`,
        ROW_NUMBER() OVER (PARTITION BY `match_id` ORDER BY `total_runs` DESC, `innings_no`, `over_id`) AS `rn`
    FROM `over_totals`
),
`bowler_per_over` AS (
    SELECT `match_id`, `innings_no`, `over_id`, `bowler`
    FROM `ball_by_ball`
    GROUP BY `match_id`, `innings_no`, `over_id`
)
SELECT 
    ro.`match_id`,
    ro.`innings_no`,
    ro.`over_id`,
    ro.`total_runs` AS `highest_over_total`,
    p.`player_name` AS `bowler_name`,
    AVG(ro.`total_runs`) OVER () AS `average_highest_over_total`
FROM `ranked_overs` ro
JOIN `bowler_per_over` bpo 
    ON ro.`match_id` = bpo.`match_id` 
    AND ro.`innings_no` = bpo.`innings_no` 
    AND ro.`over_id` = bpo.`over_id`
JOIN `player` p ON bpo.`bowler` = p.`player_id`
WHERE ro.`rn` = 1