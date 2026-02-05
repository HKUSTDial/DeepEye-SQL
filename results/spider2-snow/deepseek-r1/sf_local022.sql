WITH player_runs AS (
  SELECT 
    bbb."match_id",
    bbb."striker" AS "player_id",
    SUM(bs."runs_scored") AS "total_runs"
  FROM "IPL"."IPL"."BALL_BY_BALL" bbb
  INNER JOIN "IPL"."IPL"."BATSMAN_SCORED" bs 
    ON bbb."match_id" = bs."match_id" 
    AND bbb."innings_no" = bs."innings_no" 
    AND bbb."over_id" = bs."over_id" 
    AND bbb."ball_id" = bs."ball_id"
  GROUP BY bbb."match_id", bbb."striker"
),
player_match_team AS (
  SELECT 
    "match_id",
    "player_id",
    "team_id"
  FROM "IPL"."IPL"."PLAYER_MATCH"
),
match_winner AS (
  SELECT 
    "match_id",
    "match_winner"
  FROM "IPL"."IPL"."MATCH"
  WHERE "outcome_type" = 'Result'
)
SELECT DISTINCT 
  p."player_name"
FROM player_runs pr
INNER JOIN player_match_team pmt 
  ON pr."match_id" = pmt."match_id" 
  AND pr."player_id" = pmt."player_id"
INNER JOIN match_winner mw 
  ON pr."match_id" = mw."match_id"
INNER JOIN "IPL"."IPL"."PLAYER" p 
  ON pr."player_id" = p."player_id"
WHERE pr."total_runs" >= 100
  AND pmt."team_id" != mw."match_winner"