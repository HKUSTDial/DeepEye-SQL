WITH season_5_matches AS (
    SELECT "match_id"
    FROM "IPL"."IPL"."MATCH"
    WHERE "season_id" = 5
),
player_matches_s5 AS (
    SELECT pm."player_id", pm."match_id"
    FROM "IPL"."IPL"."PLAYER_MATCH" pm
    INNER JOIN season_5_matches s5 ON pm."match_id" = s5."match_id"
),
runs_per_player_match_s5 AS (
    SELECT bb."striker" AS "player_id", bb."match_id", SUM(bs."runs_scored") AS "runs_in_match"
    FROM "IPL"."IPL"."BALL_BY_BALL" bb
    INNER JOIN "IPL"."IPL"."BATSMAN_SCORED" bs 
        ON bb."match_id" = bs."match_id"
        AND bb."innings_no" = bs."innings_no"
        AND bb."over_id" = bs."over_id"
        AND bb."ball_id" = bs."ball_id"
    INNER JOIN season_5_matches s5 ON bb."match_id" = s5."match_id"
    GROUP BY bb."striker", bb."match_id"
),
dismissals_per_player_s5 AS (
    SELECT w."player_out" AS "player_id", COUNT(*) AS "dismissals"
    FROM "IPL"."IPL"."WICKET_TAKEN" w
    INNER JOIN season_5_matches s5 ON w."match_id" = s5."match_id"
    GROUP BY w."player_out"
),
player_runs_and_matches AS (
    SELECT 
        p."player_id",
        COUNT(DISTINCT p."match_id") AS "matches_played",
        COALESCE(SUM(r."runs_in_match"), 0) AS "total_runs"
    FROM player_matches_s5 p
    LEFT JOIN runs_per_player_match_s5 r ON p."player_id" = r."player_id" AND p."match_id" = r."match_id"
    GROUP BY p."player_id"
),
player_aggregates AS (
    SELECT 
        pr."player_id",
        pr."matches_played",
        pr."total_runs",
        COALESCE(d."dismissals", 0) AS "dismissals"
    FROM player_runs_and_matches pr
    LEFT JOIN dismissals_per_player_s5 d ON pr."player_id" = d."player_id"
),
averages AS (
    SELECT 
        "player_id",
        "matches_played",
        "total_runs",
        "dismissals",
        "total_runs" / "matches_played" AS "avg_runs_per_match",
        CASE WHEN "dismissals" > 0 THEN "total_runs" / "dismissals" ELSE NULL END AS "batting_average"
    FROM player_aggregates
)
SELECT 
    pl."player_name",
    av."avg_runs_per_match",
    av."batting_average"
FROM averages av
INNER JOIN "IPL"."IPL"."PLAYER" pl ON av."player_id" = pl."player_id"
ORDER BY av."avg_runs_per_match" DESC
LIMIT 5