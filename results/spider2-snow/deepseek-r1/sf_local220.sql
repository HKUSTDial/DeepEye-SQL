WITH match_players AS (
  SELECT "match_api_id", CAST("home_player_1" AS NUMBER) AS "player_api_id", 'home' AS "team_type", "home_team_goal", "away_team_goal" FROM "EU_SOCCER"."EU_SOCCER"."MATCH" WHERE "home_player_1" IS NOT NULL
  UNION ALL SELECT "match_api_id", CAST("home_player_2" AS NUMBER) AS "player_api_id", 'home' AS "team_type", "home_team_goal", "away_team_goal" FROM "EU_SOCCER"."EU_SOCCER"."MATCH" WHERE "home_player_2" IS NOT NULL
  UNION ALL SELECT "match_api_id", CAST("home_player_3" AS NUMBER) AS "player_api_id", 'home' AS "team_type", "home_team_goal", "away_team_goal" FROM "EU_SOCCER"."EU_SOCCER"."MATCH" WHERE "home_player_3" IS NOT NULL
  UNION ALL SELECT "match_api_id", CAST("home_player_4" AS NUMBER) AS "player_api_id", 'home' AS "team_type", "home_team_goal", "away_team_goal" FROM "EU_SOCCER"."EU_SOCCER"."MATCH" WHERE "home_player_4" IS NOT NULL
  UNION ALL SELECT "match_api_id", CAST("home_player_5" AS NUMBER) AS "player_api_id", 'home' AS "team_type", "home_team_goal", "away_team_goal" FROM "EU_SOCCER"."EU_SOCCER"."MATCH" WHERE "home_player_5" IS NOT NULL
  UNION ALL SELECT "match_api_id", CAST("home_player_6" AS NUMBER) AS "player_api_id", 'home' AS "team_type", "home_team_goal", "away_team_goal" FROM "EU_SOCCER"."EU_SOCCER"."MATCH" WHERE "home_player_6" IS NOT NULL
  UNION ALL SELECT "match_api_id", CAST("home_player_7" AS NUMBER) AS "player_api_id", 'home' AS "team_type", "home_team_goal", "away_team_goal" FROM "EU_SOCCER"."EU_SOCCER"."MATCH" WHERE "home_player_7" IS NOT NULL
  UNION ALL SELECT "match_api_id", CAST("home_player_8" AS NUMBER) AS "player_api_id", 'home' AS "team_type", "home_team_goal", "away_team_goal" FROM "EU_SOCCER"."EU_SOCCER"."MATCH" WHERE "home_player_8" IS NOT NULL
  UNION ALL SELECT "match_api_id", CAST("home_player_9" AS NUMBER) AS "player_api_id", 'home' AS "team_type", "home_team_goal", "away_team_goal" FROM "EU_SOCCER"."EU_SOCCER"."MATCH" WHERE "home_player_9" IS NOT NULL
  UNION ALL SELECT "match_api_id", CAST("home_player_10" AS NUMBER) AS "player_api_id", 'home' AS "team_type", "home_team_goal", "away_team_goal" FROM "EU_SOCCER"."EU_SOCCER"."MATCH" WHERE "home_player_10" IS NOT NULL
  UNION ALL SELECT "match_api_id", CAST("home_player_11" AS NUMBER) AS "player_api_id", 'home' AS "team_type", "home_team_goal", "away_team_goal" FROM "EU_SOCCER"."EU_SOCCER"."MATCH" WHERE "home_player_11" IS NOT NULL
  UNION ALL SELECT "match_api_id", CAST("away_player_1" AS NUMBER) AS "player_api_id", 'away' AS "team_type", "home_team_goal", "away_team_goal" FROM "EU_SOCCER"."EU_SOCCER"."MATCH" WHERE "away_player_1" IS NOT NULL
  UNION ALL SELECT "match_api_id", CAST("away_player_2" AS NUMBER) AS "player_api_id", 'away' AS "team_type", "home_team_goal", "away_team_goal" FROM "EU_SOCCER"."EU_SOCCER"."MATCH" WHERE "away_player_2" IS NOT NULL
  UNION ALL SELECT "match_api_id", CAST("away_player_3" AS NUMBER) AS "player_api_id", 'away' AS "team_type", "home_team_goal", "away_team_goal" FROM "EU_SOCCER"."EU_SOCCER"."MATCH" WHERE "away_player_3" IS NOT NULL
  UNION ALL SELECT "match_api_id", CAST("away_player_4" AS NUMBER) AS "player_api_id", 'away' AS "team_type", "home_team_goal", "away_team_goal" FROM "EU_SOCCER"."EU_SOCCER"."MATCH" WHERE "away_player_4" IS NOT NULL
  UNION ALL SELECT "match_api_id", CAST("away_player_5" AS NUMBER) AS "player_api_id", 'away' AS "team_type", "home_team_goal", "away_team_goal" FROM "EU_SOCCER"."EU_SOCCER"."MATCH" WHERE "away_player_5" IS NOT NULL
  UNION ALL SELECT "match_api_id", CAST("away_player_6" AS NUMBER) AS "player_api_id", 'away' AS "team_type", "home_team_goal", "away_team_goal" FROM "EU_SOCCER"."EU_SOCCER"."MATCH" WHERE "away_player_6" IS NOT NULL
  UNION ALL SELECT "match_api_id", CAST("away_player_7" AS NUMBER) AS "player_api_id", 'away' AS "team_type", "home_team_goal", "away_team_goal" FROM "EU_SOCCER"."EU_SOCCER"."MATCH" WHERE "away_player_7" IS NOT NULL
  UNION ALL SELECT "match_api_id", CAST("away_player_8" AS NUMBER) AS "player_api_id", 'away' AS "team_type", "home_team_goal", "away_team_goal" FROM "EU_SOCCER"."EU_SOCCER"."MATCH" WHERE "away_player_8" IS NOT NULL
  UNION ALL SELECT "match_api_id", CAST("away_player_9" AS NUMBER) AS "player_api_id", 'away' AS "team_type", "home_team_goal", "away_team_goal" FROM "EU_SOCCER"."EU_SOCCER"."MATCH" WHERE "away_player_9" IS NOT NULL
  UNION ALL SELECT "match_api_id", CAST("away_player_10" AS NUMBER) AS "player_api_id", 'away' AS "team_type", "home_team_goal", "away_team_goal" FROM "EU_SOCCER"."EU_SOCCER"."MATCH" WHERE "away_player_10" IS NOT NULL
  UNION ALL SELECT "match_api_id", CAST("away_player_11" AS NUMBER) AS "player_api_id", 'away' AS "team_type", "home_team_goal", "away_team_goal" FROM "EU_SOCCER"."EU_SOCCER"."MATCH" WHERE "away_player_11" IS NOT NULL
),
player_appearances AS (
  SELECT "match_api_id", "player_api_id", "team_type",
    CASE
      WHEN "team_type" = 'home' AND "home_team_goal" > "away_team_goal" THEN 'win'
      WHEN "team_type" = 'home' AND "home_team_goal" < "away_team_goal" THEN 'loss'
      WHEN "team_type" = 'away' AND "away_team_goal" > "home_team_goal" THEN 'win'
      WHEN "team_type" = 'away' AND "away_team_goal" < "home_team_goal" THEN 'loss'
      ELSE 'draw'
    END AS "result"
  FROM match_players
),
player_wins_losses AS (
  SELECT * FROM player_appearances WHERE "result" IN ('win', 'loss')
),
player_counts AS (
  SELECT "player_api_id",
    COUNT(CASE WHEN "result" = 'win' THEN 1 END) AS "win_count",
    COUNT(CASE WHEN "result" = 'loss' THEN 1 END) AS "loss_count"
  FROM player_wins_losses
  GROUP BY "player_api_id"
),
win_ranking AS (
  SELECT "player_api_id", "win_count", RANK() OVER (ORDER BY "win_count" DESC) AS "win_rank"
  FROM player_counts
  WHERE "win_count" > 0
),
loss_ranking AS (
  SELECT "player_api_id", "loss_count", RANK() OVER (ORDER BY "loss_count" DESC) AS "loss_rank"
  FROM player_counts
  WHERE "loss_count" > 0
)
SELECT 'win' AS "type", p."player_name", w."win_count" AS "count"
FROM win_ranking w
JOIN "EU_SOCCER"."EU_SOCCER"."PLAYER" p ON w."player_api_id" = p."player_api_id"
WHERE w."win_rank" = 1
UNION ALL
SELECT 'loss' AS "type", p."player_name", l."loss_count" AS "count"
FROM loss_ranking l
JOIN "EU_SOCCER"."EU_SOCCER"."PLAYER" p ON l."player_api_id" = p."player_api_id"
WHERE l."loss_rank" = 1