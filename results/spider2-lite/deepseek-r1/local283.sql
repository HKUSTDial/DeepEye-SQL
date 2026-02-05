WITH match_points AS (
  SELECT 
    season,
    league_id,
    home_team_api_id as team_api_id,
    CASE 
      WHEN home_team_goal > away_team_goal THEN 3
      WHEN home_team_goal = away_team_goal THEN 1
      ELSE 0
    END as points
  FROM `Match`
  UNION ALL
  SELECT 
    season,
    league_id,
    away_team_api_id as team_api_id,
    CASE 
      WHEN away_team_goal > home_team_goal THEN 3
      WHEN away_team_goal = home_team_goal THEN 1
      ELSE 0
    END as points
  FROM `Match`
),
aggregated_points AS (
  SELECT 
    season,
    league_id,
    team_api_id,
    SUM(points) as total_points
  FROM match_points
  GROUP BY season, league_id, team_api_id
),
champions AS (
  SELECT 
    season,
    league_id,
    team_api_id,
    total_points
  FROM aggregated_points
  WHERE (season, league_id, total_points) IN (
    SELECT season, league_id, MAX(total_points)
    FROM aggregated_points
    GROUP BY season, league_id
  )
)
SELECT 
  c.season,
  t.team_long_name as champion_team_name,
  l.name as league,
  co.name as country,
  c.total_points
FROM champions c
JOIN `Team` t ON c.team_api_id = t.team_api_id
JOIN `League` l ON c.league_id = l.id
JOIN `Country` co ON l.country_id = co.id
ORDER BY c.season, l.name