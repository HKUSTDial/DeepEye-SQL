SELECT
    "game_clock",
    SUM(CASE WHEN "team_id" = "home_id" THEN "points_scored" ELSE 0 END) OVER (PARTITION BY "game_id" ORDER BY "elapsed_time_sec") AS "Wildcats",
    SUM(CASE WHEN "team_id" = "away_id" THEN "points_scored" ELSE 0 END) OVER (PARTITION BY "game_id" ORDER BY "elapsed_time_sec") AS "Fighting Irish",
    CASE WHEN "team_id" = "home_id" THEN "home_name" ELSE "away_name" END AS "scoring_team",
    "event_description"
FROM "NCAA_BASKETBALL"."NCAA_BASKETBALL"."MBB_PBP_SR"
WHERE "season" = 2014
    AND "home_name" = 'Wildcats'
    AND "away_name" = 'Fighting Irish'
    AND "points_scored" IS NOT NULL
ORDER BY "elapsed_time_sec"