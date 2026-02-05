WITH game_info AS (
  SELECT game_id
  FROM `bigquery-public-data.ncaa_basketball.mbb_games_sr`
  WHERE season = 2014 AND h_name = 'Wildcats' AND a_name = 'Fighting Irish'
),
scoring_plays AS (
  SELECT 
    p.game_clock,
    p.elapsed_time_sec,
    p.event_description,
    p.team_name,
    p.home_name,
    p.away_name,
    p.points_scored,
    p.event_id,
    CASE WHEN p.team_name = p.home_name THEN p.points_scored ELSE 0 END AS home_points,
    CASE WHEN p.team_name = p.away_name THEN p.points_scored ELSE 0 END AS away_points
  FROM `bigquery-public-data.ncaa_basketball.mbb_pbp_sr` p
  INNER JOIN game_info g ON p.game_id = g.game_id
  WHERE p.points_scored > 0
)
SELECT 
  game_clock,
  SUM(home_points) OVER (ORDER BY elapsed_time_sec, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS wildcats_score,
  SUM(away_points) OVER (ORDER BY elapsed_time_sec, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS fighting_irish_score,
  team_name AS scoring_team,
  event_description
FROM scoring_plays
ORDER BY elapsed_time_sec, event_id