WITH top_venues AS (
    SELECT 
        1 AS category_order,
        'Top Venues' AS "Category",
        'N/A' AS "Date",
        "venue_name" AS "Matchup or Venue",
        MAX("venue_capacity") AS "Key Metric"
    FROM "NCAA_BASKETBALL"."NCAA_BASKETBALL"."MBB_GAMES_SR"
    WHERE "venue_capacity" IS NOT NULL
    GROUP BY "venue_name"
    ORDER BY "Key Metric" DESC
    LIMIT 5
), championship_margins AS (
    SELECT 
        2 AS category_order,
        'Biggest Championship Margins' AS "Category",
        TO_VARCHAR("game_date", 'YYYY-MM-DD') AS "Date",
        "win_market" || ' vs ' || "lose_market" AS "Matchup or Venue",
        ("win_pts" - "lose_pts") AS "Key Metric"
    FROM "NCAA_BASKETBALL"."NCAA_BASKETBALL"."MBB_HISTORICAL_TOURNAMENT_GAMES"
    WHERE "round" = 2 AND "season" > 2015 AND "win_pts" IS NOT NULL AND "lose_pts" IS NOT NULL
    ORDER BY "Key Metric" DESC
    LIMIT 5
), highest_scoring AS (
    SELECT 
        3 AS category_order,
        'Highest Scoring Games' AS "Category",
        TO_VARCHAR("scheduled_date", 'YYYY-MM-DD') AS "Date",
        "h_market" || ' vs ' || "a_market" AS "Matchup or Venue",
        ("h_points" + "a_points") AS "Key Metric"
    FROM "NCAA_BASKETBALL"."NCAA_BASKETBALL"."MBB_GAMES_SR"
    WHERE "season" > 2010 AND "h_points" IS NOT NULL AND "a_points" IS NOT NULL
    ORDER BY "Key Metric" DESC
    LIMIT 5
), total_threes AS (
    SELECT 
        4 AS category_order,
        'Total Threes' AS "Category",
        TO_VARCHAR("scheduled_date", 'YYYY-MM-DD') AS "Date",
        "h_market" || ' vs ' || "a_market" AS "Matchup or Venue",
        ("h_three_points_made" + "a_three_points_made") AS "Key Metric"
    FROM "NCAA_BASKETBALL"."NCAA_BASKETBALL"."MBB_GAMES_SR"
    WHERE "season" > 2010 AND "h_three_points_made" IS NOT NULL AND "a_three_points_made" IS NOT NULL
    ORDER BY "Key Metric" DESC
    LIMIT 5
)
SELECT 
    "Category",
    "Date",
    "Matchup or Venue",
    "Key Metric"
FROM (
    SELECT * FROM top_venues
    UNION ALL
    SELECT * FROM championship_margins
    UNION ALL
    SELECT * FROM highest_scoring
    UNION ALL
    SELECT * FROM total_threes
) AS combined
ORDER BY category_order, "Key Metric" DESC