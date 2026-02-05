WITH 
top_venues AS (
  SELECT 
    'Top Venues' AS Category,
    'N/A' AS Date,
    venue_name AS `Matchup or Venue`,
    MAX(venue_capacity) AS `Key Metric`
  FROM `bigquery-public-data.ncaa_basketball.mbb_games_sr`
  WHERE venue_capacity IS NOT NULL
  GROUP BY venue_name
  ORDER BY `Key Metric` DESC
  LIMIT 5
),
championship_margins AS (
  SELECT 
    'Biggest Championship Margins' AS Category,
    CAST(game_date AS STRING) AS Date,
    CONCAT(win_market, ' vs ', lose_market) AS `Matchup or Venue`,
    (win_pts - lose_pts) AS `Key Metric`
  FROM `bigquery-public-data.ncaa_basketball.mbb_historical_tournament_games`
  WHERE round = 2 AND season > 2015
  ORDER BY `Key Metric` DESC
  LIMIT 5
),
high_scoring AS (
  SELECT 
    'Highest Scoring Games' AS Category,
    CAST(scheduled_date AS STRING) AS Date,
    CONCAT(h_market, ' vs ', a_market) AS `Matchup or Venue`,
    (h_points + a_points) AS `Key Metric`
  FROM `bigquery-public-data.ncaa_basketball.mbb_games_sr`
  WHERE season > 2010
  ORDER BY `Key Metric` DESC
  LIMIT 5
),
total_threes AS (
  SELECT 
    'Total Threes' AS Category,
    CAST(scheduled_date AS STRING) AS Date,
    CONCAT(h_market, ' vs ', a_market) AS `Matchup or Venue`,
    (h_three_points_made + a_three_points_made) AS `Key Metric`
  FROM `bigquery-public-data.ncaa_basketball.mbb_games_sr`
  WHERE season > 2010
  ORDER BY `Key Metric` DESC
  LIMIT 5
)
SELECT 
  Category,
  Date,
  `Matchup or Venue`,
  `Key Metric`
FROM top_venues
UNION ALL
SELECT * FROM championship_margins
UNION ALL
SELECT * FROM high_scoring
UNION ALL
SELECT * FROM total_threes
ORDER BY 
  CASE Category 
    WHEN 'Top Venues' THEN 1
    WHEN 'Biggest Championship Margins' THEN 2
    WHEN 'Highest Scoring Games' THEN 3
    WHEN 'Total Threes' THEN 4
  END,
  `Key Metric` DESC