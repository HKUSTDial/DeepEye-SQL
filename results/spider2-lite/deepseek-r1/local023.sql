WITH season5_matches AS (
    SELECT match_id FROM `match` WHERE season_id = 5
),
player_matches AS (
    SELECT player_id, COUNT(DISTINCT match_id) AS matches_played
    FROM player_match
    WHERE match_id IN (SELECT match_id FROM season5_matches)
    GROUP BY player_id
),
player_runs AS (
    SELECT striker AS player_id, SUM(runs_scored) AS total_runs
    FROM ball_by_ball bb
    JOIN batsman_scored bs USING (match_id, innings_no, over_id, ball_id)
    WHERE bb.match_id IN (SELECT match_id FROM season5_matches)
    GROUP BY striker
),
player_dismissals AS (
    SELECT player_out AS player_id, COUNT(*) AS dismissals
    FROM wicket_taken
    WHERE match_id IN (SELECT match_id FROM season5_matches)
    GROUP BY player_out
)
SELECT p.player_name,
       COALESCE(pr.total_runs, 0) * 1.0 / pm.matches_played AS avg_runs_per_match,
       CASE WHEN pd.dismissals > 0 THEN COALESCE(pr.total_runs, 0) * 1.0 / pd.dismissals ELSE NULL END AS batting_avg
FROM player_matches pm
JOIN player p ON pm.player_id = p.player_id
LEFT JOIN player_runs pr ON pm.player_id = pr.player_id
LEFT JOIN player_dismissals pd ON pm.player_id = pd.player_id
ORDER BY avg_runs_per_match DESC, p.player_name
LIMIT 5;