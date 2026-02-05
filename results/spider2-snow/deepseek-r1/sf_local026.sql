WITH "over_runs" AS (
   SELECT 
      b."match_id",
      b."innings_no",
      b."over_id",
      b."bowler",
      SUM(COALESCE(bs."runs_scored", 0) + COALESCE(er."extra_runs", 0)) AS "total_runs"
   FROM "IPL"."IPL"."BALL_BY_BALL" b
   LEFT JOIN "IPL"."IPL"."BATSMAN_SCORED" bs 
        ON b."ball_id" = bs."ball_id" 
        AND b."over_id" = bs."over_id" 
        AND b."match_id" = bs."match_id" 
        AND b."innings_no" = bs."innings_no"
   LEFT JOIN "IPL"."IPL"."EXTRA_RUNS" er 
        ON b."ball_id" = er."ball_id" 
        AND b."over_id" = er."over_id" 
        AND b."match_id" = er."match_id" 
        AND b."innings_no" = er."innings_no"
   GROUP BY b."match_id", b."innings_no", b."over_id", b."bowler"
),
"match_max_over" AS (
   SELECT *,
          MAX("total_runs") OVER (PARTITION BY "match_id") AS "max_runs_in_match"
   FROM "over_runs"
),
"max_overs" AS (
   SELECT *
   FROM "match_max_over"
   WHERE "total_runs" = "max_runs_in_match"
),
"bowler_max_runs" AS (
   SELECT "bowler",
          MAX("total_runs") AS "max_bowler_runs"
   FROM "max_overs"
   GROUP BY "bowler"
),
"ranked_bowlers" AS (
   SELECT "bowler",
          "max_bowler_runs",
          RANK() OVER (ORDER BY "max_bowler_runs" DESC) AS "rank"
   FROM "bowler_max_runs"
)
SELECT 
   p."player_name" AS "bowler_name",
   rb."max_bowler_runs" AS "runs_conceded",
   mo."match_id",
   m."match_date"
FROM "ranked_bowlers" rb
INNER JOIN "IPL"."IPL"."PLAYER" p ON rb."bowler" = p."player_id"
INNER JOIN "max_overs" mo ON rb."bowler" = mo."bowler" AND rb."max_bowler_runs" = mo."total_runs"
INNER JOIN "IPL"."IPL"."MATCH" m ON mo."match_id" = m."match_id"
WHERE rb."rank" <= 3
QUALIFY ROW_NUMBER() OVER (PARTITION BY rb."bowler" ORDER BY m."match_date" ASC) = 1
ORDER BY rb."rank", m."match_date";