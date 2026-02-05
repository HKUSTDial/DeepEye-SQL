WITH historical_games AS (
    SELECT
        "season",
        'win' AS "label",
        "win_seed" AS "seed",
        "win_school_ncaa" AS "school_ncaa",
        "lose_seed" AS "opponent_seed",
        "lose_school_ncaa" AS "opponent_school_ncaa"
    FROM "NCAA_INSIGHTS"."NCAA"."MBB_HISTORICAL_TOURNAMENT_GAMES"
    WHERE "season" >= 2014 AND "season" != 2018
    UNION ALL
    SELECT
        "season",
        'loss' AS "label",
        "lose_seed" AS "seed",
        "lose_school_ncaa" AS "school_ncaa",
        "win_seed" AS "opponent_seed",
        "win_school_ncaa" AS "opponent_school_ncaa"
    FROM "NCAA_INSIGHTS"."NCAA"."MBB_HISTORICAL_TOURNAMENT_GAMES"
    WHERE "season" >= 2014 AND "season" != 2018
),
tournament_2018 AS (
    SELECT
        "season",
        "label",
        "seed",
        "school_ncaa",
        "opponent_seed",
        "opponent_school_ncaa"
    FROM "NCAA_INSIGHTS"."NCAA"."_2018_TOURNAMENT_RESULTS"
    WHERE "season" = 2018
),
all_games AS (
    SELECT * FROM historical_games
    UNION
    SELECT * FROM tournament_2018
),
games_with_team_metrics AS (
    SELECT
        ag."season",
        ag."label",
        ag."seed",
        ag."school_ncaa",
        ag."opponent_seed",
        ag."opponent_school_ncaa",
        fe."efficiency_rank",
        fe."pace_rank",
        fe."pts_100poss",
        fe."pace_rating",
        fe."efficiency_rating",
        fe."poss_40min"
    FROM all_games ag
    LEFT JOIN "NCAA_INSIGHTS"."NCAA"."FEATURE_ENGINEERING" fe
        ON ag."school_ncaa" = fe."team"
        AND ag."season" = fe."season"
)
SELECT
    g."season",
    g."label",
    g."seed",
    g."school_ncaa",
    g."opponent_seed",
    g."opponent_school_ncaa",
    g."efficiency_rank",
    g."pace_rank",
    g."pts_100poss",
    g."pace_rating",
    g."efficiency_rating",
    g."poss_40min",
    opp_fe."efficiency_rank" AS "opp_efficiency_rank",
    opp_fe."pace_rank" AS "opp_pace_rank",
    opp_fe."pts_100poss" AS "opp_pts_100poss",
    opp_fe."pace_rating" AS "opp_pace_rating",
    opp_fe."efficiency_rating" AS "opp_efficiency_rating",
    opp_fe."poss_40min" AS "opp_poss_40min",
    opp_fe."pace_rank" - g."pace_rank" AS "pace_rank_diff",
    opp_fe."poss_40min" - g."poss_40min" AS "pace_stat_diff",
    opp_fe."pace_rating" - g."pace_rating" AS "pace_rating_diff",
    opp_fe."efficiency_rank" - g."efficiency_rank" AS "eff_rank_diff",
    opp_fe."pts_100poss" - g."pts_100poss" AS "eff_stat_diff",
    opp_fe."efficiency_rating" - g."efficiency_rating" AS "eff_rating_diff"
FROM games_with_team_metrics g
LEFT JOIN "NCAA_INSIGHTS"."NCAA"."FEATURE_ENGINEERING" opp_fe
    ON g."opponent_school_ncaa" = opp_fe."team"
    AND g."season" = opp_fe."season"