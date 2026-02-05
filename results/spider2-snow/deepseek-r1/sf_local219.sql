WITH team_league_pairs AS (
  SELECT "league_id", "home_team_api_id" AS "team_api_id" FROM "EU_SOCCER"."EU_SOCCER"."MATCH"
  UNION
  SELECT "league_id", "away_team_api_id" AS "team_api_id" FROM "EU_SOCCER"."EU_SOCCER"."MATCH"
),
home_wins AS (
  SELECT "league_id", "home_team_api_id" AS "team_api_id", COUNT(*) AS "wins"
  FROM "EU_SOCCER"."EU_SOCCER"."MATCH"
  WHERE "home_team_goal" > "away_team_goal"
  GROUP BY "league_id", "home_team_api_id"
),
away_wins AS (
  SELECT "league_id", "away_team_api_id" AS "team_api_id", COUNT(*) AS "wins"
  FROM "EU_SOCCER"."EU_SOCCER"."MATCH"
  WHERE "away_team_goal" > "home_team_goal"
  GROUP BY "league_id", "away_team_api_id"
),
wins_per_team_league AS (
  SELECT 
    p."league_id",
    p."team_api_id",
    COALESCE(hw."wins", 0) + COALESCE(aw."wins", 0) AS "total_wins"
  FROM team_league_pairs p
  LEFT JOIN home_wins hw ON p."league_id" = hw."league_id" AND p."team_api_id" = hw."team_api_id"
  LEFT JOIN away_wins aw ON p."league_id" = aw."league_id" AND p."team_api_id" = aw."team_api_id"
),
min_wins_per_league AS (
  SELECT 
    "league_id",
    MIN("total_wins") AS "min_wins"
  FROM wins_per_team_league
  GROUP BY "league_id"
),
teams_with_min_wins AS (
  SELECT 
    w."league_id",
    w."team_api_id",
    w."total_wins"
  FROM wins_per_team_league w
  INNER JOIN min_wins_per_league m ON w."league_id" = m."league_id" AND w."total_wins" = m."min_wins"
),
ranked_teams AS (
  SELECT 
    "league_id",
    "team_api_id",
    "total_wins",
    ROW_NUMBER() OVER (PARTITION BY "league_id" ORDER BY "team_api_id") AS "rn"
  FROM teams_with_min_wins
)
SELECT 
  l."name" AS "league_name",
  t."team_long_name" AS "team_name",
  r."total_wins"
FROM ranked_teams r
INNER JOIN "EU_SOCCER"."EU_SOCCER"."LEAGUE" l ON r."league_id" = l."id"
INNER JOIN "EU_SOCCER"."EU_SOCCER"."TEAM" t ON r."team_api_id" = t."team_api_id"
WHERE r."rn" = 1
ORDER BY l."name"