WITH ball_runs AS (
    SELECT 
        b.match_id,
        b.innings_no,
        b.striker,
        b.non_striker,
        COALESCE(bs.runs_scored, 0) AS runs_scored
    FROM ball_by_ball b
    LEFT JOIN batsman_scored bs ON b.match_id = bs.match_id 
        AND b.innings_no = bs.innings_no 
        AND b.over_id = bs.over_id 
        AND b.ball_id = bs.ball_id
),
pair_runs AS (
    SELECT 
        match_id,
        CASE WHEN striker < non_striker THEN striker ELSE non_striker END AS player_a,
        CASE WHEN striker < non_striker THEN non_striker ELSE striker END AS player_b,
        SUM(runs_scored) AS total_runs,
        SUM(CASE WHEN striker = (CASE WHEN striker < non_striker THEN striker ELSE non_striker END) THEN runs_scored ELSE 0 END) AS runs_by_a,
        SUM(CASE WHEN striker = (CASE WHEN striker < non_striker THEN non_striker ELSE striker END) THEN runs_scored ELSE 0 END) AS runs_by_b
    FROM ball_runs
    GROUP BY match_id, 
        CASE WHEN striker < non_striker THEN striker ELSE non_striker END,
        CASE WHEN striker < non_striker THEN non_striker ELSE striker END
),
max_partnership AS (
    SELECT 
        match_id,
        MAX(total_runs) AS max_runs
    FROM pair_runs
    GROUP BY match_id
)
SELECT 
    pr.match_id,
    CASE 
        WHEN pr.runs_by_a > pr.runs_by_b THEN pr.player_a
        WHEN pr.runs_by_a < pr.runs_by_b THEN pr.player_b
        WHEN pr.player_a > pr.player_b THEN pr.player_a
        ELSE pr.player_b
    END AS player1_id,
    CASE 
        WHEN pr.runs_by_a > pr.runs_by_b THEN pr.player_b
        WHEN pr.runs_by_a < pr.runs_by_b THEN pr.player_a
        WHEN pr.player_a > pr.player_b THEN pr.player_b
        ELSE pr.player_a
    END AS player2_id,
    CASE 
        WHEN pr.runs_by_a > pr.runs_by_b THEN pr.runs_by_a
        WHEN pr.runs_by_a < pr.runs_by_b THEN pr.runs_by_b
        WHEN pr.player_a > pr.player_b THEN pr.runs_by_a
        ELSE pr.runs_by_b
    END AS player1_individual_score,
    CASE 
        WHEN pr.runs_by_a > pr.runs_by_b THEN pr.runs_by_b
        WHEN pr.runs_by_a < pr.runs_by_b THEN pr.runs_by_a
        WHEN pr.player_a > pr.player_b THEN pr.runs_by_b
        ELSE pr.runs_by_a
    END AS player2_individual_score,
    pr.total_runs AS total_partnership_score
FROM pair_runs pr
INNER JOIN max_partnership mp ON pr.match_id = mp.match_id AND pr.total_runs = mp.max_runs
ORDER BY pr.match_id, player1_id, player2_id;