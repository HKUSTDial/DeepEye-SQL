WITH batsman_runs AS (
    SELECT
        m."season_id",
        bb."striker" AS "player_id",
        SUM(bs."runs_scored") AS "total_runs"
    FROM
        "IPL"."IPL"."MATCH" m
        JOIN "IPL"."IPL"."BALL_BY_BALL" bb ON m."match_id" = bb."match_id"
        JOIN "IPL"."IPL"."BATSMAN_SCORED" bs ON bb."match_id" = bs."match_id"
            AND bb."innings_no" = bs."innings_no"
            AND bb."over_id" = bs."over_id"
            AND bb."ball_id" = bs."ball_id"
    GROUP BY
        m."season_id",
        bb."striker"
), bowler_wickets AS (
    SELECT
        m."season_id",
        bb."bowler" AS "player_id",
        COUNT(*) AS "total_wickets"
    FROM
        "IPL"."IPL"."MATCH" m
        JOIN "IPL"."IPL"."BALL_BY_BALL" bb ON m."match_id" = bb."match_id"
        JOIN "IPL"."IPL"."WICKET_TAKEN" wt ON bb."match_id" = wt."match_id"
            AND bb."innings_no" = wt."innings_no"
            AND bb."over_id" = wt."over_id"
            AND bb."ball_id" = wt."ball_id"
    WHERE
        wt."kind_out" NOT IN ('run out', 'hit wicket', 'retired hurt')
    GROUP BY
        m."season_id",
        bb."bowler"
), ranked_batsmen AS (
    SELECT
        "season_id",
        "player_id",
        "total_runs",
        ROW_NUMBER() OVER (PARTITION BY "season_id" ORDER BY "total_runs" DESC, "player_id" ASC) AS "batsman_rank"
    FROM
        batsman_runs
), ranked_bowlers AS (
    SELECT
        "season_id",
        "player_id",
        "total_wickets",
        ROW_NUMBER() OVER (PARTITION BY "season_id" ORDER BY "total_wickets" DESC, "player_id" ASC) AS "bowler_rank"
    FROM
        bowler_wickets
)
SELECT
    rb."season_id",
    rb."player_id" AS "batsman_id",
    p_batsman."player_name" AS "batsman_name",
    rb."total_runs",
    rbow."player_id" AS "bowler_id",
    p_bowler."player_name" AS "bowler_name",
    rbow."total_wickets",
    rb."batsman_rank" AS "rank_position"
FROM
    ranked_batsmen rb
    JOIN ranked_bowlers rbow ON rb."season_id" = rbow."season_id" AND rb."batsman_rank" = rbow."bowler_rank"
    LEFT JOIN "IPL"."IPL"."PLAYER" p_batsman ON rb."player_id" = p_batsman."player_id"
    LEFT JOIN "IPL"."IPL"."PLAYER" p_bowler ON rbow."player_id" = p_bowler."player_id"
WHERE
    rb."batsman_rank" <= 3
    AND rbow."bowler_rank" <= 3
ORDER BY
    rb."season_id" ASC,
    rb."batsman_rank" ASC