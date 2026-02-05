WITH valid_shots AS (
  SELECT 
    game_id,
    shot_type,
    CASE WHEN event_coord_x < 564 THEN event_coord_x ELSE 1128 - event_coord_x END AS adj_x,
    CASE WHEN event_coord_x < 564 THEN 600 - event_coord_y ELSE event_coord_y END AS adj_y,
    shot_made
  FROM `bigquery-public-data.ncaa_basketball.mbb_pbp_sr`
  WHERE scheduled_date < TIMESTAMP('2018-03-15')
    AND shot_type IS NOT NULL
    AND shot_type != 'NULL'
    AND event_coord_x IS NOT NULL
    AND event_coord_y IS NOT NULL
    AND ((team_basket = 'left' AND event_coord_x < 564) OR (team_basket = 'right' AND event_coord_x >= 564))
),
coord_agg AS (
  SELECT 
    shot_type,
    AVG(adj_x) AS avg_x,
    AVG(adj_y) AS avg_y
  FROM valid_shots
  GROUP BY shot_type
),
game_agg AS (
  SELECT 
    shot_type,
    game_id,
    COUNT(*) AS attempts_per_game,
    SUM(CAST(shot_made AS INT64)) AS successful_per_game
  FROM valid_shots
  GROUP BY shot_type, game_id
),
game_avg AS (
  SELECT 
    shot_type,
    AVG(attempts_per_game) AS avg_shot_attempts,
    AVG(successful_per_game) AS avg_successful_shots
  FROM game_agg
  GROUP BY shot_type
)
SELECT 
  c.shot_type,
  c.avg_x,
  c.avg_y,
  g.avg_shot_attempts,
  g.avg_successful_shots
FROM coord_agg c
JOIN game_avg g ON c.shot_type = g.shot_type
ORDER BY c.shot_type;