WITH over_runs AS (
    SELECT 
        b.`match_id`,
        b.`innings_no`,
        b.`over_id`,
        b.`bowler`,
        SUM(COALESCE(bs.`runs_scored`,0) + COALESCE(er.`extra_runs`,0)) AS `over_runs`
    FROM `ball_by_ball` b
    LEFT JOIN `batsman_scored` bs ON b.`match_id` = bs.`match_id` AND b.`innings_no` = bs.`innings_no` AND b.`over_id` = bs.`over_id` AND b.`ball_id` = bs.`ball_id`
    LEFT JOIN `extra_runs` er ON b.`match_id` = er.`match_id` AND b.`innings_no` = er.`innings_no` AND b.`over_id` = er.`over_id` AND b.`ball_id` = er.`ball_id`
    GROUP BY b.`match_id`, b.`innings_no`, b.`over_id`, b.`bowler`
),
max_runs_per_match AS (
    SELECT `match_id`, MAX(`over_runs`) AS `max_runs`
    FROM over_runs
    GROUP BY `match_id`
),
qualifying_overs AS (
    SELECT 
        oruns.`match_id`,
        oruns.`innings_no`,
        oruns.`over_id`,
        oruns.`bowler`,
        oruns.`over_runs`
    FROM over_runs oruns
    JOIN max_runs_per_match mrpm ON oruns.`match_id` = mrpm.`match_id` AND oruns.`over_runs` = mrpm.`max_runs`
),
bowler_max_match AS (
    SELECT 
        `bowler`,
        `over_runs`,
        `match_id`,
        ROW_NUMBER() OVER (PARTITION BY `bowler` ORDER BY `over_runs` DESC, `match_id`) AS `rn_bowler`
    FROM qualifying_overs
),
filtered_bowler_max AS (
    SELECT `bowler`, `over_runs` AS `max_over_runs`, `match_id`
    FROM bowler_max_match
    WHERE `rn_bowler` = 1
),
ranked_bowlers AS (
    SELECT 
        `bowler`,
        `max_over_runs`,
        `match_id`,
        ROW_NUMBER() OVER (ORDER BY `max_over_runs` DESC, `bowler`) AS `rn`
    FROM filtered_bowler_max
)
SELECT 
    p.`player_name`,
    m.`match_date`,
    rb.`max_over_runs` AS `runs_conceded`
FROM ranked_bowlers rb
JOIN `player` p ON rb.`bowler` = p.`player_id`
JOIN `match` m ON rb.`match_id` = m.`match_id`
WHERE rb.`rn` <= 3
ORDER BY rb.`rn`