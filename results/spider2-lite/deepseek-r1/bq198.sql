WITH season_max AS (
  SELECT
    `season`,
    MAX(`wins`) AS max_wins
  FROM `bigquery-public-data.ncaa_basketball.mbb_historical_teams_seasons`
  WHERE `season` BETWEEN 1900 AND 2000
    AND `market` IS NOT NULL
    AND `name` IS NOT NULL
  GROUP BY `season`
),
teams_with_max AS (
  SELECT DISTINCT
    t.`market`,
    t.`season`
  FROM `bigquery-public-data.ncaa_basketball.mbb_historical_teams_seasons` t
  INNER JOIN season_max s
    ON t.`season` = s.`season` AND t.`wins` = s.`max_wins`
  WHERE t.`season` BETWEEN 1900 AND 2000
    AND t.`market` IS NOT NULL
    AND t.`name` IS NOT NULL
)
SELECT
  `market` AS university,
  COUNT(DISTINCT `season`) AS peak_seasons_count
FROM teams_with_max
GROUP BY `market`
ORDER BY peak_seasons_count DESC
LIMIT 5