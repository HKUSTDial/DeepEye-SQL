WITH striker_match_runs AS (
    SELECT
        bb."match_id",
        bb."striker",
        SUM(bs."runs_scored") AS total_runs
    FROM
        "IPL"."IPL"."BALL_BY_BALL" bb
    JOIN
        "IPL"."IPL"."BATSMAN_SCORED" bs
    ON
        bb."ball_id" = bs."ball_id"
        AND bb."over_id" = bs."over_id"
        AND bb."match_id" = bs."match_id"
        AND bb."innings_no" = bs."innings_no"
    GROUP BY
        bb."match_id",
        bb."striker"
    HAVING
        total_runs > 50
)
SELECT
    AVG(total_runs) AS average_runs
FROM
    striker_match_runs