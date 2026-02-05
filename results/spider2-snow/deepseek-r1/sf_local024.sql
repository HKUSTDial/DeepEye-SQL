WITH PlayerMatchRuns AS (
    SELECT 
        pm."player_id",
        pm."match_id",
        COALESCE(runs."total_runs_in_match", 0) AS "runs_in_match"
    FROM "IPL"."IPL"."PLAYER_MATCH" pm
    LEFT JOIN (
        SELECT 
            bbb."match_id",
            bbb."striker" AS "player_id",
            SUM(bs."runs_scored") AS "total_runs_in_match"
        FROM "IPL"."IPL"."BALL_BY_BALL" bbb
        INNER JOIN "IPL"."IPL"."BATSMAN_SCORED" bs 
            ON bbb."match_id" = bs."match_id" 
            AND bbb."innings_no" = bs."innings_no"
            AND bbb."over_id" = bs."over_id"
            AND bbb."ball_id" = bs."ball_id"
        GROUP BY bbb."match_id", bbb."striker"
    ) runs ON pm."match_id" = runs."match_id" AND pm."player_id" = runs."player_id"
),
PlayerAvg AS (
    SELECT 
        "player_id",
        SUM("runs_in_match") / COUNT("match_id") AS "avg_runs_per_match"
    FROM PlayerMatchRuns
    GROUP BY "player_id"
),
CountryAvg AS (
    SELECT 
        p."country_name",
        AVG(pa."avg_runs_per_match") AS "country_batting_average"
    FROM PlayerAvg pa
    INNER JOIN "IPL"."IPL"."PLAYER" p ON pa."player_id" = p."player_id"
    WHERE p."country_name" IS NOT NULL
    GROUP BY p."country_name"
)
SELECT 
    "country_name",
    "country_batting_average"
FROM CountryAvg
ORDER BY "country_batting_average" DESC
LIMIT 5