WITH match_points AS (
  SELECT 
    "league_id",
    "season",
    "home_team_api_id" AS "team_api_id",
    CASE 
      WHEN "home_team_goal" > "away_team_goal" THEN 3
      WHEN "home_team_goal" = "away_team_goal" THEN 1
      ELSE 0
    END AS "points"
  FROM "EU_SOCCER"."EU_SOCCER"."MATCH"
  UNION ALL
  SELECT 
    "league_id",
    "season",
    "away_team_api_id" AS "team_api_id",
    CASE 
      WHEN "away_team_goal" > "home_team_goal" THEN 3
      WHEN "away_team_goal" = "home_team_goal" THEN 1
      ELSE 0
    END AS "points"
  FROM "EU_SOCCER"."EU_SOCCER"."MATCH"
),
aggregated_points AS (
  SELECT 
    "league_id",
    "season",
    "team_api_id",
    SUM("points") AS "total_points"
  FROM match_points
  GROUP BY "league_id", "season", "team_api_id"
),
ranked_teams AS (
  SELECT 
    "league_id",
    "season",
    "team_api_id",
    "total_points",
    RANK() OVER (PARTITION BY "league_id", "season" ORDER BY "total_points" DESC) AS "rank"
  FROM aggregated_points
)
SELECT 
  t."team_long_name" AS "champion_team_name",
  l."name" AS "league_name",
  c."name" AS "country_name",
  r."season",
  r."total_points"
FROM ranked_teams r
JOIN "EU_SOCCER"."EU_SOCCER"."TEAM" t ON r."team_api_id" = t."team_api_id"
JOIN "EU_SOCCER"."EU_SOCCER"."LEAGUE" l ON r."league_id" = l."id"
JOIN "EU_SOCCER"."EU_SOCCER"."COUNTRY" c ON l."country_id" = c."id"
WHERE r."rank" = 1
ORDER BY r."season", l."name"