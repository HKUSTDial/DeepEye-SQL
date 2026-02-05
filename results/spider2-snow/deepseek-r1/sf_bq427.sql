WITH filtered_shots AS (
  SELECT 
    "game_id",
    "shot_type",
    CASE WHEN "event_coord_x" < 564 THEN "event_coord_x" ELSE 1128 - "event_coord_x" END AS "adjusted_x",
    CASE WHEN "event_coord_x" < 564 THEN 600 - "event_coord_y" ELSE "event_coord_y" END AS "adjusted_y",
    "shot_made"
  FROM "NCAA_BASKETBALL"."NCAA_BASKETBALL"."MBB_PBP_SR"
  WHERE "scheduled_date" < 1521072000000000
    AND "shot_type" IS NOT NULL
    AND "event_coord_x" IS NOT NULL
    AND "event_coord_y" IS NOT NULL
),
per_game_stats AS (
  SELECT 
    "game_id",
    "shot_type",
    AVG("adjusted_x") AS "avg_x_per_game",
    AVG("adjusted_y") AS "avg_y_per_game",
    COUNT(*) AS "shot_attempts_per_game",
    SUM(CASE WHEN "shot_made" THEN 1 ELSE 0 END) AS "successful_shots_per_game"
  FROM filtered_shots
  GROUP BY "game_id", "shot_type"
)
SELECT 
  "shot_type",
  AVG("avg_x_per_game") AS "avg_x",
  AVG("avg_y_per_game") AS "avg_y",
  AVG("shot_attempts_per_game") AS "avg_shot_attempts",
  AVG("successful_shots_per_game") AS "avg_successful_shots"
FROM per_game_stats
GROUP BY "shot_type"
ORDER BY "shot_type"