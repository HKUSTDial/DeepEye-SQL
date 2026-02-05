WITH bowler_wickets_per_match AS (
    SELECT bbb.bowler, bbb.match_id, COUNT(*) AS wickets
    FROM wicket_taken wt
    JOIN ball_by_ball bbb ON wt.match_id = bbb.match_id 
        AND wt.innings_no = bbb.innings_no 
        AND wt.over_id = bbb.over_id 
        AND wt.ball_id = bbb.ball_id
    WHERE wt.kind_out IN ('bowled', 'caught', 'lbw', 'stumped', 'hit wicket')
    GROUP BY bbb.bowler, bbb.match_id
),
bowler_runs_per_match AS (
    SELECT 
        bbb.bowler,
        bbb.match_id,
        SUM(COALESCE(bs.runs_scored, 0)) AS runs_conceded,
        SUM(CASE WHEN er.extra_type IS NULL OR er.extra_type NOT IN ('wides', 'noball') THEN 1 ELSE 0 END) AS legal_balls_bowled
    FROM ball_by_ball bbb
    LEFT JOIN batsman_scored bs ON bbb.match_id = bs.match_id 
        AND bbb.innings_no = bs.innings_no 
        AND bbb.over_id = bs.over_id 
        AND bbb.ball_id = bs.ball_id
    LEFT JOIN extra_runs er ON bbb.match_id = er.match_id 
        AND bbb.innings_no = er.innings_no 
        AND bbb.over_id = er.over_id 
        AND bbb.ball_id = er.ball_id
    GROUP BY bbb.bowler, bbb.match_id
),
bowler_match_stats AS (
    SELECT 
        br.bowler,
        br.match_id,
        COALESCE(bw.wickets, 0) AS wickets,
        br.runs_conceded,
        br.legal_balls_bowled
    FROM bowler_runs_per_match br
    LEFT JOIN bowler_wickets_per_match bw ON br.bowler = bw.bowler AND br.match_id = bw.match_id
),
bowler_totals AS (
    SELECT 
        bowler,
        SUM(wickets) AS total_wickets,
        SUM(runs_conceded) AS total_runs,
        SUM(legal_balls_bowled) AS total_legal_balls
    FROM bowler_match_stats
    GROUP BY bowler
),
best_bowling_per_bowler AS (
    SELECT 
        bowler,
        wickets,
        runs_conceded,
        ROW_NUMBER() OVER (PARTITION BY bowler ORDER BY wickets DESC, runs_conceded ASC) AS rn
    FROM bowler_match_stats
)
SELECT 
    p.player_name,
    bt.total_wickets,
    CASE WHEN bt.total_legal_balls > 0 THEN ROUND(bt.total_runs * 6.0 / bt.total_legal_balls, 2) ELSE NULL END AS economy_rate,
    CASE WHEN bt.total_wickets > 0 THEN ROUND(CAST(bt.total_legal_balls AS REAL) / bt.total_wickets, 2) ELSE NULL END AS strike_rate,
    (bb.wickets || '-' || bb.runs_conceded) AS best_bowling
FROM bowler_totals bt
JOIN player p ON bt.bowler = p.player_id
LEFT JOIN (SELECT bowler, wickets, runs_conceded FROM best_bowling_per_bowler WHERE rn = 1) bb ON bt.bowler = bb.bowler
ORDER BY p.player_name;