WITH player_period_points AS (
    SELECT "game_id", "player_id", "team_market", SUM("points_scored") AS total_points
    FROM "NCAA_BASKETBALL"."NCAA_BASKETBALL"."MBB_PBP_SR"
    WHERE "period" = 2 AND "season" BETWEEN 2010 AND 2018 AND "player_id" IS NOT NULL
    GROUP BY "game_id", "player_id", "team_market"
    HAVING total_points >= 15
),
market_player_counts AS (
    SELECT "team_market", COUNT(DISTINCT "player_id") AS distinct_players
    FROM player_period_points
    GROUP BY "team_market"
    ORDER BY distinct_players DESC
    LIMIT 5
),
top_markets AS (
    SELECT "team_market" FROM market_player_counts
)
SELECT 
    "season",
    "round",
    "days_from_epoch",
    "game_date",
    "day",
    'win' AS "label",
    "win_seed" AS "seed",
    "win_market" AS "market",
    "win_name" AS "name",
    "win_alias" AS "alias",
    "win_school_ncaa" AS "school_ncaa",
    "lose_seed" AS "opponent_seed",
    "lose_market" AS "opponent_market",
    "lose_name" AS "opponent_name",
    "lose_alias" AS "opponent_alias",
    "lose_school_ncaa" AS "opponent_school_ncaa"
FROM "NCAA_BASKETBALL"."NCAA_BASKETBALL"."MBB_HISTORICAL_TOURNAMENT_GAMES"
WHERE "win_market" IN (SELECT "team_market" FROM top_markets)
    AND "season" BETWEEN 2010 AND 2018
UNION ALL
SELECT 
    "season",
    "round",
    "days_from_epoch",
    "game_date",
    "day",
    'loss' AS "label",
    "lose_seed" AS "seed",
    "lose_market" AS "market",
    "lose_name" AS "name",
    "lose_alias" AS "alias",
    "lose_school_ncaa" AS "school_ncaa",
    "win_seed" AS "opponent_seed",
    "win_market" AS "opponent_market",
    "win_name" AS "opponent_name",
    "win_alias" AS "opponent_alias",
    "win_school_ncaa" AS "opponent_school_ncaa"
FROM "NCAA_BASKETBALL"."NCAA_BASKETBALL"."MBB_HISTORICAL_TOURNAMENT_GAMES"
WHERE "lose_market" IN (SELECT "team_market" FROM top_markets)
    AND "season" BETWEEN 2010 AND 2018
ORDER BY "season", "round", "game_date", "market"