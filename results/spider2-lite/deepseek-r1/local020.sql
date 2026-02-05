WITH runs_conceded AS (
    SELECT
        bbb.`bowler`,
        SUM(COALESCE(bs.`runs_scored`, 0)) + SUM(CASE WHEN er.`extra_type` IN ('wides', 'noballs') THEN COALESCE(er.`extra_runs`, 0) ELSE 0 END) AS `runs_conceded`
    FROM `ball_by_ball` bbb
    LEFT JOIN `batsman_scored` bs ON bbb.`match_id` = bs.`match_id` AND bbb.`innings_no` = bs.`innings_no` AND bbb.`over_id` = bs.`over_id` AND bbb.`ball_id` = bs.`ball_id`
    LEFT JOIN `extra_runs` er ON bbb.`match_id` = er.`match_id` AND bbb.`innings_no` = er.`innings_no` AND bbb.`over_id` = er.`over_id` AND bbb.`ball_id` = er.`ball_id`
    GROUP BY bbb.`bowler`
),
wickets_taken AS (
    SELECT
        bbb.`bowler`,
        COUNT(*) AS `wickets_taken`
    FROM `ball_by_ball` bbb
    INNER JOIN `wicket_taken` wt ON bbb.`match_id` = wt.`match_id` AND bbb.`innings_no` = wt.`innings_no` AND bbb.`over_id` = wt.`over_id` AND bbb.`ball_id` = wt.`ball_id`
    WHERE wt.`kind_out` IN ('bowled', 'caught', 'lbw')
    GROUP BY bbb.`bowler`
),
bowling_avg AS (
    SELECT
        rc.`bowler`,
        rc.`runs_conceded` * 1.0 / wt.`wickets_taken` AS `bowling_average`
    FROM runs_conceded rc
    JOIN wickets_taken wt ON rc.`bowler` = wt.`bowler`
)
SELECT p.`player_name`, ba.`bowling_average`
FROM bowling_avg ba
JOIN `player` p ON ba.`bowler` = p.`player_id`
ORDER BY ba.`bowling_average` ASC
LIMIT 1