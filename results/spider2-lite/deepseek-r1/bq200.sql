WITH all_games AS (
  SELECT * FROM `bigquery-public-data.baseball.games_wide`
  UNION ALL
  SELECT * FROM `bigquery-public-data.baseball.games_post_wide`
),
pitcher_team_assignments AS (
  SELECT 
    gameId,
    pitcherId,
    pitcherFirstName,
    pitcherLastName,
    pitchSpeed,
    CASE 
      WHEN pitcherId IN (homeFielder1, homeFielder2, homeFielder3, homeFielder4, homeFielder5, homeFielder6,
                         homeFielder7, homeFielder8, homeFielder9, homeFielder10, homeFielder11, homeFielder12,
                         homeBatter1, homeBatter2, homeBatter3, homeBatter4, homeBatter5, homeBatter6,
                         homeBatter7, homeBatter8, homeBatter9) THEN 'HOME'
      WHEN pitcherId IN (awayFielder1, awayFielder2, awayFielder3, awayFielder4, awayFielder5, awayFielder6,
                         awayFielder7, awayFielder8, awayFielder9, awayFielder10, awayFielder11, awayFielder12,
                         awayBatter1, awayBatter2, awayBatter3, awayBatter4, awayBatter5, awayBatter6,
                         awayBatter7, awayBatter8, awayBatter9) THEN 'AWAY'
      ELSE NULL 
    END AS pitcher_side,
    homeTeamId,
    homeTeamName,
    awayTeamId,
    awayTeamName
  FROM all_games
  WHERE pitchSpeed > 0
),
valid_pitches AS (
  SELECT 
    pitcherId,
    pitcherFirstName,
    pitcherLastName,
    CASE 
      WHEN pitcher_side = 'HOME' THEN homeTeamId
      WHEN pitcher_side = 'AWAY' THEN awayTeamId
    END AS teamId,
    CASE 
      WHEN pitcher_side = 'HOME' THEN homeTeamName
      WHEN pitcher_side = 'AWAY' THEN awayTeamName
    END AS teamName,
    pitchSpeed
  FROM pitcher_team_assignments
  WHERE pitcher_side IS NOT NULL
),
pitcher_team_max_speed AS (
  SELECT 
    teamId,
    teamName,
    pitcherId,
    pitcherFirstName,
    pitcherLastName,
    MAX(pitchSpeed) AS max_pitch_speed
  FROM valid_pitches
  GROUP BY teamId, teamName, pitcherId, pitcherFirstName, pitcherLastName
),
ranked_pitchers AS (
  SELECT 
    teamId,
    teamName,
    pitcherId,
    pitcherFirstName,
    pitcherLastName,
    max_pitch_speed,
    ROW_NUMBER() OVER (PARTITION BY teamId ORDER BY max_pitch_speed DESC) AS rn
  FROM pitcher_team_max_speed
)
SELECT 
  teamId,
  teamName,
  CONCAT(pitcherFirstName, ' ', pitcherLastName) AS pitcher_full_name,
  max_pitch_speed
FROM ranked_pitchers
WHERE rn = 1
ORDER BY teamId;