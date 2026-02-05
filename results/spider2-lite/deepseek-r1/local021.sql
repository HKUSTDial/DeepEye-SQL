WITH match_runs AS (
    SELECT bbb.match_id, bbb.striker, SUM(bs.runs_scored) AS runs_in_match
    FROM ball_by_ball bbb
    JOIN batsman_scored bs ON bbb.match_id = bs.match_id 
        AND bbb.innings_no = bs.innings_no 
        AND bbb.over_id = bs.over_id 
        AND bbb.ball_id = bs.ball_id
    GROUP BY bbb.match_id, bbb.striker
),
qualifying_strikers AS (
    SELECT DISTINCT striker
    FROM match_runs
    WHERE runs_in_match > 50
),
striker_totals AS (
    SELECT mr.striker, SUM(mr.runs_in_match) AS total_runs
    FROM match_runs mr
    WHERE mr.striker IN (SELECT striker FROM qualifying_strikers)
    GROUP BY mr.striker
)
SELECT AVG(total_runs) AS average_total_runs
FROM striker_totals