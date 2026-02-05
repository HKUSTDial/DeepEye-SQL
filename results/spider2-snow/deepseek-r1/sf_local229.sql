WITH "ball_data" AS (
    SELECT 
        "bbb"."match_id",
        "bbb"."innings_no",
        CASE WHEN "bbb"."striker" > "bbb"."non_striker" THEN "bbb"."striker" ELSE "bbb"."non_striker" END AS "player1_id",
        CASE WHEN "bbb"."striker" > "bbb"."non_striker" THEN "bbb"."non_striker" ELSE "bbb"."striker" END AS "player2_id",
        "bbb"."striker",
        COALESCE("bs"."runs_scored", 0) AS "runs_scored"
    FROM "IPL"."IPL"."BALL_BY_BALL" AS "bbb"
    LEFT JOIN "IPL"."IPL"."BATSMAN_SCORED" AS "bs"
        ON "bbb"."match_id" = "bs"."match_id"
        AND "bbb"."innings_no" = "bs"."innings_no"
        AND "bbb"."over_id" = "bs"."over_id"
        AND "bbb"."ball_id" = "bs"."ball_id"
),
"partnership_scores" AS (
    SELECT 
        "match_id",
        "innings_no",
        "player1_id",
        "player2_id",
        SUM(CASE WHEN "striker" = "player1_id" THEN "runs_scored" ELSE 0 END) AS "player1_runs",
        SUM(CASE WHEN "striker" = "player2_id" THEN "runs_scored" ELSE 0 END) AS "player2_runs",
        SUM("runs_scored") AS "partnership_runs"
    FROM "ball_data"
    GROUP BY "match_id", "innings_no", "player1_id", "player2_id"
    HAVING "partnership_runs" > 0
),
"ordered_partnerships" AS (
    SELECT 
        "match_id",
        "innings_no",
        CASE 
            WHEN "player1_runs" > "player2_runs" THEN "player1_id"
            WHEN "player2_runs" > "player1_runs" THEN "player2_id"
            WHEN "player1_id" > "player2_id" THEN "player1_id"
            ELSE "player2_id"
        END AS "player1",
        CASE 
            WHEN "player1_runs" > "player2_runs" THEN "player2_id"
            WHEN "player2_runs" > "player1_runs" THEN "player1_id"
            WHEN "player1_id" > "player2_id" THEN "player2_id"
            ELSE "player1_id"
        END AS "player2",
        CASE 
            WHEN "player1_runs" > "player2_runs" THEN "player1_runs"
            WHEN "player2_runs" > "player1_runs" THEN "player2_runs"
            WHEN "player1_id" > "player2_id" THEN "player1_runs"
            ELSE "player2_runs"
        END AS "player1_score",
        CASE 
            WHEN "player1_runs" > "player2_runs" THEN "player2_runs"
            WHEN "player2_runs" > "player1_runs" THEN "player1_runs"
            WHEN "player1_id" > "player2_id" THEN "player2_runs"
            ELSE "player1_runs"
        END AS "player2_score",
        "partnership_runs" AS "total_partnership_score"
    FROM "partnership_scores"
),
"max_partnerships" AS (
    SELECT 
        "match_id",
        MAX("total_partnership_score") AS "max_partnership_score"
    FROM "ordered_partnerships"
    GROUP BY "match_id"
)
SELECT 
    "op"."match_id",
    "op"."player1",
    "op"."player2",
    "op"."player1_score",
    "op"."player2_score",
    "op"."total_partnership_score"
FROM "ordered_partnerships" AS "op"
INNER JOIN "max_partnerships" AS "mp"
    ON "op"."match_id" = "mp"."match_id"
    AND "op"."total_partnership_score" = "mp"."max_partnership_score"
ORDER BY "op"."match_id", "op"."player1", "op"."player2"