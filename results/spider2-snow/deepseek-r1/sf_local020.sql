WITH bat_runs_per_ball AS (
    SELECT 
        "match_id",
        "innings_no",
        "over_id",
        "ball_id",
        SUM("runs_scored") AS "runs_scored"
    FROM 
        "IPL"."IPL"."BATSMAN_SCORED"
    GROUP BY 
        "match_id",
        "innings_no",
        "over_id",
        "ball_id"
), extra_runs_per_ball AS (
    SELECT 
        "match_id",
        "innings_no",
        "over_id",
        "ball_id",
        SUM("extra_runs") AS "extra_runs"
    FROM 
        "IPL"."IPL"."EXTRA_RUNS"
    GROUP BY 
        "match_id",
        "innings_no",
        "over_id",
        "ball_id"
), bowler_runs AS (
    SELECT 
        bbb."bowler" AS "bowler_id",
        SUM(COALESCE(b."runs_scored", 0) + COALESCE(e."extra_runs", 0)) AS "total_runs"
    FROM 
        "IPL"."IPL"."BALL_BY_BALL" bbb
    LEFT JOIN 
        bat_runs_per_ball b 
        ON bbb."match_id" = b."match_id" 
        AND bbb."innings_no" = b."innings_no" 
        AND bbb."over_id" = b."over_id" 
        AND bbb."ball_id" = b."ball_id"
    LEFT JOIN 
        extra_runs_per_ball e 
        ON bbb."match_id" = e."match_id" 
        AND bbb."innings_no" = e."innings_no" 
        AND bbb."over_id" = e."over_id" 
        AND bbb."ball_id" = e."ball_id"
    GROUP BY 
        bbb."bowler"
), bowler_wickets AS (
    SELECT 
        bbb."bowler" AS "bowler_id",
        COUNT(*) AS "total_wickets"
    FROM 
        "IPL"."IPL"."BALL_BY_BALL" bbb
    INNER JOIN 
        "IPL"."IPL"."WICKET_TAKEN" wt 
        ON bbb."match_id" = wt."match_id" 
        AND bbb."innings_no" = wt."innings_no" 
        AND bbb."over_id" = wt."over_id" 
        AND bbb."ball_id" = wt."ball_id"
    GROUP BY 
        bbb."bowler"
), bowler_avg AS (
    SELECT 
        bw."bowler_id",
        br."total_runs",
        bw."total_wickets",
        br."total_runs" / bw."total_wickets" AS "bowling_average"
    FROM 
        bowler_wickets bw
    INNER JOIN 
        bowler_runs br 
        ON bw."bowler_id" = br."bowler_id"
    WHERE 
        bw."total_wickets" > 0
)
SELECT 
    p."player_name"
FROM 
    bowler_avg ba
INNER JOIN 
    "IPL"."IPL"."PLAYER" p 
    ON ba."bowler_id" = p."player_id"
ORDER BY 
    ba."bowling_average" ASC
LIMIT 1