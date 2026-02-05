WITH ball_data AS (
    SELECT
        b."bowler",
        b."match_id",
        CASE WHEN e."extra_type" IN ('wide', 'no ball') THEN 0 ELSE 1 END AS is_legal,
        COALESCE(bs."runs_scored", 0) AS runs_off_bat,
        CASE WHEN w."kind_out" IN ('caught', 'bowled', 'lbw', 'stumped', 'hit wicket') THEN 1 ELSE 0 END AS is_wicket
    FROM "IPL"."IPL"."BALL_BY_BALL" b
    LEFT JOIN "IPL"."IPL"."EXTRA_RUNS" e
        ON b."ball_id" = e."ball_id"
        AND b."match_id" = e."match_id"
        AND b."innings_no" = e."innings_no"
        AND b."over_id" = e."over_id"
    LEFT JOIN "IPL"."IPL"."BATSMAN_SCORED" bs
        ON b."ball_id" = bs."ball_id"
        AND b."match_id" = bs."match_id"
        AND b."innings_no" = bs."innings_no"
        AND b."over_id" = bs."over_id"
    LEFT JOIN "IPL"."IPL"."WICKET_TAKEN" w
        ON b."ball_id" = w."ball_id"
        AND b."match_id" = w."match_id"
        AND b."innings_no" = w."innings_no"
        AND b."over_id" = w."over_id"
),
bowler_match_stats AS (
    SELECT
        "bowler",
        "match_id",
        SUM(is_legal) AS legal_balls,
        SUM(runs_off_bat) AS runs_conceded,
        SUM(is_wicket) AS wickets_taken
    FROM ball_data
    GROUP BY "bowler", "match_id"
),
bowler_overall AS (
    SELECT
        "bowler",
        SUM(wickets_taken) AS total_wickets,
        SUM(runs_conceded) AS total_runs,
        SUM(legal_balls) AS total_legal_balls,
        CASE
            WHEN SUM(legal_balls) > 0 THEN SUM(runs_conceded) / (SUM(legal_balls) / 6.0)
            ELSE NULL
        END AS economy_rate,
        CASE
            WHEN SUM(wickets_taken) > 0 THEN SUM(legal_balls) / SUM(wickets_taken)
            ELSE NULL
        END AS strike_rate
    FROM bowler_match_stats
    GROUP BY "bowler"
),
best_performance AS (
    SELECT
        "bowler",
        "match_id",
        wickets_taken,
        runs_conceded,
        ROW_NUMBER() OVER (PARTITION BY "bowler" ORDER BY wickets_taken DESC, runs_conceded ASC) AS rn
    FROM bowler_match_stats
    WHERE wickets_taken > 0
)
SELECT
    p."player_name",
    bo.total_wickets,
    bo.economy_rate,
    bo.strike_rate,
    bp.wickets_taken || '-' || bp.runs_conceded AS best_bowling
FROM bowler_overall bo
LEFT JOIN best_performance bp ON bo."bowler" = bp."bowler" AND bp.rn = 1
INNER JOIN "IPL"."IPL"."PLAYER" p ON bo."bowler" = p."player_id"
ORDER BY bo.total_wickets DESC, bo.economy_rate ASC, bo.strike_rate ASC;