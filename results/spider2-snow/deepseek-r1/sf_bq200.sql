WITH combined AS (
  SELECT * FROM "MLB"."BASEBALL"."GAMES_WIDE"
  UNION ALL
  SELECT * FROM "MLB"."BASEBALL"."GAMES_POST_WIDE"
), games_with_team AS (
  SELECT
    "pitcherId",
    "pitcherFirstName",
    "pitcherLastName",
    "pitchSpeed",
    "homeTeamId",
    "homeTeamName",
    "awayTeamId",
    "awayTeamName",
    ARRAY_CONSTRUCT(
      "homeBatter1", "homeBatter2", "homeBatter3", "homeBatter4", "homeBatter5", "homeBatter6", "homeBatter7", "homeBatter8", "homeBatter9",
      "homeFielder1", "homeFielder2", "homeFielder3", "homeFielder4", "homeFielder5", "homeFielder6", "homeFielder7", "homeFielder8", "homeFielder9", "homeFielder10", "homeFielder11", "homeFielder12"
    ) AS home_players,
    ARRAY_CONSTRUCT(
      "awayBatter1", "awayBatter2", "awayBatter3", "awayBatter4", "awayBatter5", "awayBatter6", "awayBatter7", "awayBatter8", "awayBatter9",
      "awayFielder1", "awayFielder2", "awayFielder3", "awayFielder4", "awayFielder5", "awayFielder6", "awayFielder7", "awayFielder8", "awayFielder9", "awayFielder10", "awayFielder11", "awayFielder12"
    ) AS away_players
  FROM combined
  WHERE "pitchSpeed" > 0
), pitcher_team AS (
  SELECT
    "pitcherId",
    "pitcherFirstName",
    "pitcherLastName",
    "pitchSpeed",
    CASE
      WHEN ARRAY_CONTAINS("pitcherId"::VARIANT, home_players) THEN "homeTeamId"
      WHEN ARRAY_CONTAINS("pitcherId"::VARIANT, away_players) THEN "awayTeamId"
    END AS "teamId",
    CASE
      WHEN ARRAY_CONTAINS("pitcherId"::VARIANT, home_players) THEN "homeTeamName"
      WHEN ARRAY_CONTAINS("pitcherId"::VARIANT, away_players) THEN "awayTeamName"
    END AS "teamName"
  FROM games_with_team
  WHERE "teamId" IS NOT NULL
), pitcher_max_speed AS (
  SELECT
    "teamId",
    "teamName",
    "pitcherId",
    "pitcherFirstName",
    "pitcherLastName",
    MAX("pitchSpeed") AS "maxSpeed"
  FROM pitcher_team
  GROUP BY "teamId", "teamName", "pitcherId", "pitcherFirstName", "pitcherLastName"
), ranked AS (
  SELECT
    "teamId",
    "teamName",
    "pitcherId",
    "pitcherFirstName",
    "pitcherLastName",
    "maxSpeed",
    RANK() OVER (PARTITION BY "teamId" ORDER BY "maxSpeed" DESC) AS rnk
  FROM pitcher_max_speed
)
SELECT
  "teamName",
  "pitcherFirstName" || ' ' || "pitcherLastName" AS "pitcherFullName",
  "maxSpeed"
FROM ranked
WHERE rnk = 1