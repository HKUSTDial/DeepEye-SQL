WITH player_roles AS (
    SELECT 
        "player_id" AS player_id,
        "role" AS role,
        ROW_NUMBER() OVER (PARTITION BY "player_id" ORDER BY COUNT(*) DESC) AS rn
    FROM "IPL"."IPL"."PLAYER_MATCH"
    WHERE "role" IS NOT NULL
    GROUP BY "player_id", "role"
),
most_frequent_role AS (
    SELECT 
        player_id,
        role AS most_frequent_role
    FROM player_roles
    WHERE rn = 1
),
batting_per_match AS (
    SELECT 
        bb."striker" AS player_id,
        bb."match_id" AS match_id,
        SUM(bs."runs_scored") AS runs_in_match,
        COUNT(*) AS balls_faced_in_match
    FROM "IPL"."IPL"."BALL_BY_BALL" bb
    JOIN "IPL"."IPL"."BATSMAN_SCORED" bs
        ON bb."match_id" = bs."match_id"
        AND bb."innings_no" = bs."innings_no"
        AND bb."over_id" = bs."over_id"
        AND bb."ball_id" = bs."ball_id"
    GROUP BY bb."striker", bb."match_id"
),
player_batting AS (
    SELECT
        bpm.player_id,
        SUM(bpm.runs_in_match) AS total_runs,
        COUNT(DISTINCT bpm.match_id) AS batting_matches,
        MAX(bpm.runs_in_match) AS highest_score,
        SUM(CASE WHEN bpm.runs_in_match >= 30 THEN 1 ELSE 0 END) AS matches_30plus,
        SUM(CASE WHEN bpm.runs_in_match >= 50 THEN 1 ELSE 0 END) AS matches_50plus,
        SUM(CASE WHEN bpm.runs_in_match >= 100 THEN 1 ELSE 0 END) AS matches_100plus,
        SUM(bpm.balls_faced_in_match) AS total_balls_faced
    FROM batting_per_match bpm
    GROUP BY bpm.player_id
),
dismissals AS (
    SELECT
        wt."player_out" AS player_id,
        COUNT(*) AS total_dismissals
    FROM "IPL"."IPL"."WICKET_TAKEN" wt
    GROUP BY wt."player_out"
),
bowling_per_match AS (
    SELECT
        bb."bowler" AS player_id,
        bb."match_id" AS match_id,
        COUNT(wt."player_out") AS wickets_in_match,
        SUM(bs."runs_scored") AS runs_conceded_in_match,
        COUNT(*) AS balls_bowled_in_match
    FROM "IPL"."IPL"."BALL_BY_BALL" bb
    LEFT JOIN "IPL"."IPL"."WICKET_TAKEN" wt
        ON bb."match_id" = wt."match_id"
        AND bb."innings_no" = wt."innings_no"
        AND bb."over_id" = wt."over_id"
        AND bb."ball_id" = wt."ball_id"
    LEFT JOIN "IPL"."IPL"."BATSMAN_SCORED" bs
        ON bb."match_id" = bs."match_id"
        AND bb."innings_no" = bs."innings_no"
        AND bb."over_id" = bs."over_id"
        AND bb."ball_id" = bs."ball_id"
    GROUP BY bb."bowler", bb."match_id"
),
player_bowling AS (
    SELECT
        bpm.player_id,
        SUM(bpm.wickets_in_match) AS total_wickets,
        SUM(bpm.runs_conceded_in_match) AS total_runs_conceded,
        SUM(bpm.balls_bowled_in_match) AS total_balls_bowled,
        MAX(bpm.wickets_in_match) AS max_wickets_in_match
    FROM bowling_per_match bpm
    GROUP BY bpm.player_id
),
best_bowling_raw AS (
    SELECT
        bpm.player_id,
        bpm.wickets_in_match,
        bpm.runs_conceded_in_match,
        ROW_NUMBER() OVER (
            PARTITION BY bpm.player_id 
            ORDER BY bpm.wickets_in_match DESC, bpm.runs_conceded_in_match ASC
        ) AS rn
    FROM bowling_per_match bpm
    WHERE bpm.wickets_in_match > 0
),
best_bowling AS (
    SELECT
        bbr.player_id,
        bbr.wickets_in_match || '-' || bbr.runs_conceded_in_match AS best_bowling_performance
    FROM best_bowling_raw bbr
    WHERE bbr.rn = 1
),
matches_played AS (
    SELECT
        "player_id" AS player_id,
        COUNT(DISTINCT "match_id") AS total_matches_played
    FROM "IPL"."IPL"."PLAYER_MATCH"
    GROUP BY "player_id"
)
SELECT
    p."player_id",
    p."player_name",
    mfr.most_frequent_role,
    p."batting_hand",
    p."bowling_skill",
    COALESCE(pb.total_runs, 0) AS total_runs,
    COALESCE(mp.total_matches_played, 0) AS total_matches_played,
    COALESCE(d.total_dismissals, 0) AS total_dismissals,
    CASE 
        WHEN COALESCE(d.total_dismissals, 0) > 0 
        THEN ROUND(COALESCE(pb.total_runs, 0)::FLOAT / d.total_dismissals, 2)
        ELSE NULL 
    END AS batting_average,
    COALESCE(pb.highest_score, 0) AS highest_score,
    COALESCE(pb.matches_30plus, 0) AS matches_at_least_30_runs,
    COALESCE(pb.matches_50plus, 0) AS matches_at_least_50_runs,
    COALESCE(pb.matches_100plus, 0) AS matches_at_least_100_runs,
    COALESCE(pb.total_balls_faced, 0) AS total_balls_faced,
    CASE 
        WHEN COALESCE(pb.total_balls_faced, 0) > 0 
        THEN ROUND((COALESCE(pb.total_runs, 0)::FLOAT / pb.total_balls_faced) * 100, 2)
        ELSE NULL 
    END AS strike_rate,
    COALESCE(pbow.total_wickets, 0) AS total_wickets_taken,
    CASE 
        WHEN COALESCE(pbow.total_balls_bowled, 0) > 0 
        THEN ROUND((COALESCE(pbow.total_runs_conceded, 0)::FLOAT / pbow.total_balls_bowled) * 6, 2)
        ELSE NULL 
    END AS economy_rate,
    COALESCE(bb.best_bowling_performance, '0-0') AS best_bowling_performance
FROM "IPL"."IPL"."PLAYER" p
LEFT JOIN most_frequent_role mfr ON p."player_id" = mfr.player_id
LEFT JOIN matches_played mp ON p."player_id" = mp.player_id
LEFT JOIN player_batting pb ON p."player_id" = pb.player_id
LEFT JOIN dismissals d ON p."player_id" = d.player_id
LEFT JOIN player_bowling pbow ON p."player_id" = pbow.player_id
LEFT JOIN best_bowling bb ON p."player_id" = bb.player_id
ORDER BY p."player_id"