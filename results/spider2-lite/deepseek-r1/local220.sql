WITH player_appearances AS (
  SELECT match_api_id, home_player_1 AS player_api_id, home_team_goal AS team_goals, away_team_goal AS opponent_goals
  FROM `Match`
  WHERE home_player_1 IS NOT NULL
  UNION ALL
  SELECT match_api_id, home_player_2, home_team_goal, away_team_goal
  FROM `Match`
  WHERE home_player_2 IS NOT NULL
  UNION ALL
  SELECT match_api_id, home_player_3, home_team_goal, away_team_goal
  FROM `Match`
  WHERE home_player_3 IS NOT NULL
  UNION ALL
  SELECT match_api_id, home_player_4, home_team_goal, away_team_goal
  FROM `Match`
  WHERE home_player_4 IS NOT NULL
  UNION ALL
  SELECT match_api_id, home_player_5, home_team_goal, away_team_goal
  FROM `Match`
  WHERE home_player_5 IS NOT NULL
  UNION ALL
  SELECT match_api_id, home_player_6, home_team_goal, away_team_goal
  FROM `Match`
  WHERE home_player_6 IS NOT NULL
  UNION ALL
  SELECT match_api_id, home_player_7, home_team_goal, away_team_goal
  FROM `Match`
  WHERE home_player_7 IS NOT NULL
  UNION ALL
  SELECT match_api_id, home_player_8, home_team_goal, away_team_goal
  FROM `Match`
  WHERE home_player_8 IS NOT NULL
  UNION ALL
  SELECT match_api_id, home_player_9, home_team_goal, away_team_goal
  FROM `Match`
  WHERE home_player_9 IS NOT NULL
  UNION ALL
  SELECT match_api_id, home_player_10, home_team_goal, away_team_goal
  FROM `Match`
  WHERE home_player_10 IS NOT NULL
  UNION ALL
  SELECT match_api_id, home_player_11, home_team_goal, away_team_goal
  FROM `Match`
  WHERE home_player_11 IS NOT NULL
  UNION ALL
  SELECT match_api_id, away_player_1, away_team_goal, home_team_goal
  FROM `Match`
  WHERE away_player_1 IS NOT NULL
  UNION ALL
  SELECT match_api_id, away_player_2, away_team_goal, home_team_goal
  FROM `Match`
  WHERE away_player_2 IS NOT NULL
  UNION ALL
  SELECT match_api_id, away_player_3, away_team_goal, home_team_goal
  FROM `Match`
  WHERE away_player_3 IS NOT NULL
  UNION ALL
  SELECT match_api_id, away_player_4, away_team_goal, home_team_goal
  FROM `Match`
  WHERE away_player_4 IS NOT NULL
  UNION ALL
  SELECT match_api_id, away_player_5, away_team_goal, home_team_goal
  FROM `Match`
  WHERE away_player_5 IS NOT NULL
  UNION ALL
  SELECT match_api_id, away_player_6, away_team_goal, home_team_goal
  FROM `Match`
  WHERE away_player_6 IS NOT NULL
  UNION ALL
  SELECT match_api_id, away_player_7, away_team_goal, home_team_goal
  FROM `Match`
  WHERE away_player_7 IS NOT NULL
  UNION ALL
  SELECT match_api_id, away_player_8, away_team_goal, home_team_goal
  FROM `Match`
  WHERE away_player_8 IS NOT NULL
  UNION ALL
  SELECT match_api_id, away_player_9, away_team_goal, home_team_goal
  FROM `Match`
  WHERE away_player_9 IS NOT NULL
  UNION ALL
  SELECT match_api_id, away_player_10, away_team_goal, home_team_goal
  FROM `Match`
  WHERE away_player_10 IS NOT NULL
  UNION ALL
  SELECT match_api_id, away_player_11, away_team_goal, home_team_goal
  FROM `Match`
  WHERE away_player_11 IS NOT NULL
),
player_counts AS (
  SELECT 
    player_api_id,
    COUNT(DISTINCT CASE WHEN team_goals > opponent_goals THEN match_api_id END) AS win_count,
    COUNT(DISTINCT CASE WHEN team_goals < opponent_goals THEN match_api_id END) AS loss_count
  FROM player_appearances
  GROUP BY player_api_id
),
max_wins AS (
  SELECT MAX(win_count) AS max_win_count FROM player_counts
),
max_losses AS (
  SELECT MAX(loss_count) AS max_loss_count FROM player_counts
)
SELECT 
  p.player_name,
  pc.win_count AS count,
  'most_wins' AS type
FROM player_counts pc
JOIN `Player` p ON pc.player_api_id = p.player_api_id
CROSS JOIN max_wins mw
WHERE pc.win_count = mw.max_win_count
UNION ALL
SELECT 
  p.player_name,
  pc.loss_count AS count,
  'most_losses' AS type
FROM player_counts pc
JOIN `Player` p ON pc.player_api_id = p.player_api_id
CROSS JOIN max_losses ml
WHERE pc.loss_count = ml.max_loss_count