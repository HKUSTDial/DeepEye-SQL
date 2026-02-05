WITH batsman_runs AS (
  SELECT m.season_id, bbb.striker AS player_id, SUM(bs.runs_scored) AS total_runs
  FROM `match` m
  JOIN `ball_by_ball` bbb ON m.match_id = bbb.match_id
  JOIN `batsman_scored` bs ON bbb.match_id = bs.match_id AND bbb.innings_no = bs.innings_no AND bbb.over_id = bs.over_id AND bbb.ball_id = bs.ball_id
  GROUP BY m.season_id, bbb.striker
),
ranked_batsmen AS (
  SELECT season_id, player_id, total_runs,
    ROW_NUMBER() OVER (PARTITION BY season_id ORDER BY total_runs DESC, player_id ASC) AS batsman_rank
  FROM batsman_runs
),
top_batsmen AS (
  SELECT * FROM ranked_batsmen WHERE batsman_rank <= 3
),
bowler_wickets AS (
  SELECT m.season_id, bbb.bowler AS player_id, COUNT(*) AS total_wickets
  FROM `match` m
  JOIN `wicket_taken` wt ON m.match_id = wt.match_id
  JOIN `ball_by_ball` bbb ON wt.match_id = bbb.match_id AND wt.innings_no = bbb.innings_no AND wt.over_id = bbb.over_id AND wt.ball_id = bbb.ball_id
  WHERE wt.kind_out NOT IN ('run out', 'hit wicket', 'retired hurt')
  GROUP BY m.season_id, bbb.bowler
),
ranked_bowlers AS (
  SELECT season_id, player_id, total_wickets,
    ROW_NUMBER() OVER (PARTITION BY season_id ORDER BY total_wickets DESC, player_id ASC) AS bowler_rank
  FROM bowler_wickets
),
top_bowlers AS (
  SELECT * FROM ranked_bowlers WHERE bowler_rank <= 3
)
SELECT 
  b.season_id,
  b.player_id AS batsman_id,
  p_batsman.player_name AS batsman_name,
  b.total_runs AS batsman_total_runs,
  bow.player_id AS bowler_id,
  p_bowler.player_name AS bowler_name,
  bow.total_wickets AS bowler_total_wickets
FROM top_batsmen b
JOIN top_bowlers bow ON b.season_id = bow.season_id AND b.batsman_rank = bow.bowler_rank
JOIN `player` p_batsman ON b.player_id = p_batsman.player_id
JOIN `player` p_bowler ON bow.player_id = p_bowler.player_id
ORDER BY b.season_id ASC, b.batsman_rank ASC