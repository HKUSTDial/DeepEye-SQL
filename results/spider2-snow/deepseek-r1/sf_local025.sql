WITH base_overs AS (
    SELECT
        "match_id",
        "innings_no",
        "over_id",
        MIN("bowler") AS "bowler"
    FROM "IPL"."IPL"."BALL_BY_BALL"
    GROUP BY "match_id", "innings_no", "over_id"
),
bs_runs AS (
    SELECT
        "match_id",
        "innings_no",
        "over_id",
        SUM("runs_scored") AS "batsman_runs"
    FROM "IPL"."IPL"."BATSMAN_SCORED"
    GROUP BY "match_id", "innings_no", "over_id"
),
er_runs AS (
    SELECT
        "match_id",
        "innings_no",
        "over_id",
        SUM("extra_runs") AS "extra_runs"
    FROM "IPL"."IPL"."EXTRA_RUNS"
    GROUP BY "match_id", "innings_no", "over_id"
),
over_runs AS (
    SELECT
        bo."match_id",
        bo."innings_no",
        bo."over_id",
        bo."bowler",
        COALESCE(bs."batsman_runs", 0) AS "batsman_runs",
        COALESCE(er."extra_runs", 0) AS "extra_runs",
        COALESCE(bs."batsman_runs", 0) + COALESCE(er."extra_runs", 0) AS "total_runs"
    FROM base_overs bo
    LEFT JOIN bs_runs bs
        ON bo."match_id" = bs."match_id"
        AND bo."innings_no" = bs."innings_no"
        AND bo."over_id" = bs."over_id"
    LEFT JOIN er_runs er
        ON bo."match_id" = er."match_id"
        AND bo."innings_no" = er."innings_no"
        AND bo."over_id" = er."over_id"
),
ranked_overs AS (
    SELECT
        "match_id",
        "innings_no",
        "over_id",
        "total_runs",
        "bowler",
        ROW_NUMBER() OVER (PARTITION BY "match_id" ORDER BY "total_runs" DESC, "innings_no", "over_id") AS "rn"
    FROM over_runs
)
SELECT
    ro."match_id",
    ro."innings_no",
    ro."over_id",
    ro."total_runs" AS "highest_over_total",
    p."player_name" AS "bowler_name",
    AVG(ro."total_runs") OVER() AS "average_highest_over_total"
FROM ranked_overs ro
LEFT JOIN "IPL"."IPL"."PLAYER" p ON ro."bowler" = p."player_id"
WHERE ro."rn" = 1