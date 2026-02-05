WITH player_period_points AS (
  SELECT
    game_id,
    team_market,
    player_id,
    SUM(points_scored) AS period_points
  FROM `bigquery-public-data.ncaa_basketball.mbb_pbp_sr`
  WHERE season BETWEEN 2010 AND 2018
    AND period = 2
    AND points_scored IS NOT NULL
  GROUP BY game_id, team_market, player_id
  HAVING period_points >= 15
),
distinct_players_per_market AS (
  SELECT
    team_market,
    COUNT(DISTINCT player_id) AS player_count
  FROM player_period_points
  WHERE team_market IS NOT NULL
  GROUP BY team_market
),
top_markets AS (
  SELECT team_market
  FROM distinct_players_per_market
  ORDER BY player_count DESC
  LIMIT 5
),
tournament_games AS (
  SELECT
    season,
    round,
    days_from_epoch,
    game_date,
    day,
    'win' AS label,
    win_seed AS seed,
    win_market AS market,
    win_name AS name,
    win_alias AS alias,
    win_school_ncaa AS school_ncaa,
    lose_seed AS opponent_seed,
    lose_market AS opponent_market,
    lose_name AS opponent_name,
    lose_alias AS opponent_alias,
    lose_school_ncaa AS opponent_school_ncaa
  FROM `bigquery-public-data.ncaa_basketball.mbb_historical_tournament_games`
  WHERE season BETWEEN 2010 AND 2018
  UNION ALL
  SELECT
    season,
    round,
    days_from_epoch,
    game_date,
    day,
    'loss' AS label,
    lose_seed AS seed,
    lose_market AS market,
    lose_name AS name,
    lose_alias AS alias,
    lose_school_ncaa AS school_ncaa,
    win_seed AS opponent_seed,
    win_market AS opponent_market,
    win_name AS opponent_name,
    win_alias AS opponent_alias,
    win_school_ncaa AS opponent_school_ncaa
  FROM `bigquery-public-data.ncaa_basketball.mbb_historical_tournament_games`
  WHERE season BETWEEN 2010 AND 2018
)
SELECT
  tg.season,
  tg.round,
  tg.days_from_epoch,
  tg.game_date,
  tg.day,
  tg.label,
  tg.seed,
  tg.market,
  tg.name,
  tg.alias,
  tg.school_ncaa,
  tg.opponent_seed,
  tg.opponent_market,
  tg.opponent_name,
  tg.opponent_alias,
  tg.opponent_school_ncaa
FROM tournament_games tg
INNER JOIN top_markets tm ON tg.market = tm.team_market
ORDER BY tg.market, tg.game_date, tg.label