WITH season_goals AS (
    SELECT 
        "team_api_id",
        "season",
        SUM("goals") AS "total_goals"
    FROM (
        SELECT "home_team_api_id" AS "team_api_id", "season", "home_team_goal" AS "goals" FROM "EU_SOCCER"."EU_SOCCER"."MATCH"
        UNION ALL
        SELECT "away_team_api_id" AS "team_api_id", "season", "away_team_goal" AS "goals" FROM "EU_SOCCER"."EU_SOCCER"."MATCH"
    ) g
    GROUP BY "team_api_id", "season"
),
highest_season_goals_per_team AS (
    SELECT 
        "team_api_id",
        MAX("total_goals") AS "highest_season_goals"
    FROM season_goals
    GROUP BY "team_api_id"
)
SELECT MEDIAN("highest_season_goals") AS "median_highest_season_goals"
FROM highest_season_goals_per_team