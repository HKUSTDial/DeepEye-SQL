WITH player_role_mode AS (
    SELECT `player_id`, `role` AS `most_frequent_role`
    FROM (
        SELECT `player_id`, `role`,
               ROW_NUMBER() OVER (PARTITION BY `player_id` ORDER BY COUNT(*) DESC) AS rn
        FROM `player_match`
        GROUP BY `player_id`, `role`
    ) t
    WHERE rn = 1
),
matches_played AS (
    SELECT `player_id`, COUNT(DISTINCT `match_id`) AS `total_matches_played`
    FROM `player_match`
    GROUP BY `player_id`
),
ball_batsman_runs AS (
    SELECT bbb.`match_id`, bbb.`innings_no`, bbb.`over_id`, bbb.`ball_id`, bbb.`striker`, bs.`runs_scored`
    FROM `ball_by_ball` bbb
    JOIN `batsman_scored` bs ON bbb.`match_id` = bs.`match_id` AND bbb.`innings_no` = bs.`innings_no` AND bbb.`over_id` = bs.`over_id` AND bbb.`ball_id` = bs.`ball_id`
),
batting_agg AS (
    SELECT `striker` AS `player_id`,
           SUM(`runs_scored`) AS `total_runs`,
           COUNT(*) AS `total_balls_faced`
    FROM `ball_batsman_runs`
    GROUP BY `striker`
),
batting_per_match AS (
    SELECT `striker` AS `player_id`, `match_id`,
           SUM(`runs_scored`) AS `runs_in_match`
    FROM `ball_batsman_runs`
    GROUP BY `striker`, `match_id`
),
batting_high_and_thresholds AS (
    SELECT `player_id`,
           MAX(`runs_in_match`) AS `highest_score`,
           COUNT(CASE WHEN `runs_in_match` >= 30 THEN 1 END) AS `matches_at_least_30`,
           COUNT(CASE WHEN `runs_in_match` >= 50 THEN 1 END) AS `matches_at_least_50`,
           COUNT(CASE WHEN `runs_in_match` >= 100 THEN 1 END) AS `matches_at_least_100`
    FROM `batting_per_match`
    GROUP BY `player_id`
),
dismissal_agg AS (
    SELECT `player_out` AS `player_id`, COUNT(*) AS `total_dismissals`
    FROM `wicket_taken`
    GROUP BY `player_out`
),
ball_bowling_details AS (
    SELECT 
        bbb.`match_id`, bbb.`innings_no`, bbb.`over_id`, bbb.`ball_id`, bbb.`bowler`,
        COALESCE(bs.`runs_scored`, 0) AS `runs_scored`,
        CASE WHEN wt.`ball_id` IS NOT NULL THEN 1 ELSE 0 END AS `wicket`
    FROM `ball_by_ball` bbb
    LEFT JOIN `batsman_scored` bs ON bbb.`match_id` = bs.`match_id` AND bbb.`innings_no` = bs.`innings_no` AND bbb.`over_id` = bs.`over_id` AND bbb.`ball_id` = bs.`ball_id`
    LEFT JOIN `wicket_taken` wt ON bbb.`match_id` = wt.`match_id` AND bbb.`innings_no` = wt.`innings_no` AND bbb.`over_id` = wt.`over_id` AND bbb.`ball_id` = wt.`ball_id`
),
bowling_agg AS (
    SELECT `bowler` AS `player_id`,
           SUM(`runs_scored`) AS `total_runs_conceded`,
           COUNT(*) AS `total_balls_bowled`
    FROM `ball_bowling_details`
    GROUP BY `bowler`
),
wickets_agg AS (
    SELECT `bowler` AS `player_id`, COUNT(*) AS `total_wickets`
    FROM `ball_bowling_details`
    WHERE `wicket` = 1
    GROUP BY `bowler`
),
bowling_per_match AS (
    SELECT 
        `bowler` AS `player_id`,
        `match_id`,
        SUM(`runs_scored`) AS `runs_conceded_in_match`,
        SUM(`wicket`) AS `wickets_in_match`
    FROM `ball_bowling_details`
    GROUP BY `bowler`, `match_id`
),
best_bowling AS (
    SELECT `player_id`, 
           `wickets_in_match` || '-' || `runs_conceded_in_match` AS `best_bowling_performance`
    FROM (
        SELECT `player_id`, `wickets_in_match`, `runs_conceded_in_match`,
               ROW_NUMBER() OVER (PARTITION BY `player_id` ORDER BY `wickets_in_match` DESC, `runs_conceded_in_match` ASC) AS rn
        FROM `bowling_per_match`
    ) t
    WHERE rn = 1
)
SELECT 
    p.`player_id`,
    p.`player_name`,
    prm.`most_frequent_role`,
    p.`batting_hand`,
    p.`bowling_skill`,
    COALESCE(ba.`total_runs`, 0) AS `total_runs_scored`,
    COALESCE(mp.`total_matches_played`, 0) AS `total_matches_played`,
    COALESCE(da.`total_dismissals`, 0) AS `total_times_dismissed`,
    CASE 
        WHEN COALESCE(da.`total_dismissals`, 0) > 0 
        THEN ROUND(1.0 * COALESCE(ba.`total_runs`, 0) / da.`total_dismissals`, 2)
        ELSE NULL 
    END AS `batting_average`,
    COALESCE(bht.`highest_score`, 0) AS `highest_score`,
    COALESCE(bht.`matches_at_least_30`, 0) AS `matches_at_least_30_runs`,
    COALESCE(bht.`matches_at_least_50`, 0) AS `matches_at_least_50_runs`,
    COALESCE(bht.`matches_at_least_100`, 0) AS `matches_at_least_100_runs`,
    COALESCE(ba.`total_balls_faced`, 0) AS `total_balls_faced`,
    CASE 
        WHEN COALESCE(ba.`total_balls_faced`, 0) > 0 
        THEN ROUND(100.0 * COALESCE(ba.`total_runs`, 0) / ba.`total_balls_faced`, 2)
        ELSE NULL 
    END AS `strike_rate`,
    COALESCE(wa.`total_wickets`, 0) AS `total_wickets_taken`,
    CASE 
        WHEN COALESCE(bowa.`total_balls_bowled`, 0) > 0 
        THEN ROUND(6.0 * COALESCE(bowa.`total_runs_conceded`, 0) / bowa.`total_balls_bowled`, 2)
        ELSE NULL 
    END AS `economy_rate`,
    COALESCE(bb.`best_bowling_performance`, '0-0') AS `best_bowling_performance`
FROM `player` p
LEFT JOIN player_role_mode prm ON p.`player_id` = prm.`player_id`
LEFT JOIN matches_played mp ON p.`player_id` = mp.`player_id`
LEFT JOIN batting_agg ba ON p.`player_id` = ba.`player_id`
LEFT JOIN batting_high_and_thresholds bht ON p.`player_id` = bht.`player_id`
LEFT JOIN dismissal_agg da ON p.`player_id` = da.`player_id`
LEFT JOIN bowling_agg bowa ON p.`player_id` = bowa.`player_id`
LEFT JOIN wickets_agg wa ON p.`player_id` = wa.`player_id`
LEFT JOIN best_bowling bb ON p.`player_id` = bb.`player_id`
ORDER BY p.`player_id`;