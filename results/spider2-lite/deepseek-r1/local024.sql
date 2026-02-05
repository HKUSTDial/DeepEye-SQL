WITH player_total_runs AS (
    SELECT 
        pm.player_id,
        SUM(COALESCE(bs.runs_scored, 0)) AS total_runs
    FROM player_match pm
    LEFT JOIN ball_by_ball bbb ON pm.match_id = bbb.match_id 
        AND pm.player_id = bbb.striker
    LEFT JOIN batsman_scored bs ON bbb.match_id = bs.match_id 
        AND bbb.innings_no = bs.innings_no 
        AND bbb.over_id = bs.over_id 
        AND bbb.ball_id = bs.ball_id
    GROUP BY pm.player_id
),
player_match_count AS (
    SELECT 
        player_id,
        COUNT(*) AS matches_played
    FROM player_match
    GROUP BY player_id
),
player_averages AS (
    SELECT 
        ptr.player_id,
        ptr.total_runs * 1.0 / pmc.matches_played AS player_avg_runs_per_match
    FROM player_total_runs ptr
    JOIN player_match_count pmc ON ptr.player_id = pmc.player_id
)
SELECT 
    p.country_name,
    AVG(pa.player_avg_runs_per_match) AS country_batting_average
FROM player_averages pa
JOIN player p ON pa.player_id = p.player_id
GROUP BY p.country_name
ORDER BY country_batting_average DESC
LIMIT 5