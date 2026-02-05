WITH historical_games AS (
  SELECT 
    season,
    win_school_ncaa AS school_ncaa,
    win_seed AS seed,
    'win' AS label,
    lose_school_ncaa AS opponent_school_ncaa,
    lose_seed AS opponent_seed
  FROM `data-to-insights.ncaa.mbb_historical_tournament_games`
  WHERE season >= 2014
  
  UNION ALL
  
  SELECT 
    season,
    lose_school_ncaa AS school_ncaa,
    lose_seed AS seed,
    'loss' AS label,
    win_school_ncaa AS opponent_school_ncaa,
    win_seed AS opponent_seed
  FROM `data-to-insights.ncaa.mbb_historical_tournament_games`
  WHERE season >= 2014
),
all_games AS (
  SELECT 
    season,
    school_ncaa,
    seed,
    label,
    opponent_school_ncaa,
    opponent_seed
  FROM historical_games
  
  UNION ALL
  
  SELECT 
    season,
    school_ncaa,
    seed,
    label,
    opponent_school_ncaa,
    opponent_seed
  FROM `data-to-insights.ncaa.2018_tournament_results`
)
SELECT 
  g.season,
  g.label,
  g.seed,
  g.school_ncaa,
  g.opponent_seed,
  g.opponent_school_ncaa,
  t.pace_rank,
  t.poss_40min,
  t.pace_rating,
  t.efficiency_rank,
  t.pts_100poss,
  t.efficiency_rating,
  o.pace_rank AS opp_pace_rank,
  o.poss_40min AS opp_poss_40min,
  o.pace_rating AS opp_pace_rating,
  o.efficiency_rank AS opp_efficiency_rank,
  o.pts_100poss AS opp_pts_100poss,
  o.efficiency_rating AS opp_efficiency_rating,
  (o.pace_rank - t.pace_rank) AS pace_rank_diff,
  (o.poss_40min - t.poss_40min) AS pace_stat_diff,
  (o.pace_rating - t.pace_rating) AS pace_rating_diff,
  (o.efficiency_rank - t.efficiency_rank) AS eff_rank_diff,
  (o.pts_100poss - t.pts_100poss) AS eff_stat_diff,
  (o.efficiency_rating - t.efficiency_rating) AS eff_rating_diff
FROM all_games g
LEFT JOIN `data-to-insights.ncaa.feature_engineering` t
  ON g.school_ncaa = t.team AND g.season = t.season
LEFT JOIN `data-to-insights.ncaa.feature_engineering` o
  ON g.opponent_school_ncaa = o.team AND g.season = o.season