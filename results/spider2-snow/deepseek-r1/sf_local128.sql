WITH qualified_games AS (
  SELECT
    bs."BowlerID",
    bs."MatchID",
    bs."GameNumber",
    bs."HandiCapScore",
    t."TourneyLocation",
    t."TourneyDate"
  FROM (SELECT * FROM "BOWLINGLEAGUE"."BOWLINGLEAGUE"."BOWLER_SCORES" UNION ALL SELECT * FROM "BOWLINGLEAGUE"."BOWLINGLEAGUE"."BOWLER_SCORES_ARCHIVE") bs
  INNER JOIN (SELECT * FROM "BOWLINGLEAGUE"."BOWLINGLEAGUE"."TOURNEY_MATCHES" UNION ALL SELECT * FROM "BOWLINGLEAGUE"."BOWLINGLEAGUE"."TOURNEY_MATCHES_ARCHIVE") tm ON bs."MatchID" = tm."MatchID"
  INNER JOIN (SELECT * FROM "BOWLINGLEAGUE"."BOWLINGLEAGUE"."TOURNAMENTS" UNION ALL SELECT * FROM "BOWLINGLEAGUE"."BOWLINGLEAGUE"."TOURNAMENTS_ARCHIVE") t ON tm."TourneyID" = CAST(t."TourneyID" AS NUMBER)
  WHERE bs."WonGame" = 1
    AND bs."HandiCapScore" <= 190
    AND t."TourneyLocation" IN ('Thunderbird Lanes', 'Totem Lanes', 'Bolero Lanes')
),
bowlers_with_all_venues AS (
  SELECT "BowlerID"
  FROM qualified_games
  GROUP BY "BowlerID"
  HAVING COUNT(DISTINCT "TourneyLocation") = 3
)
SELECT
  b."BowlerID",
  b."BowlerFirstName",
  b."BowlerLastName",
  qg."MatchID",
  qg."GameNumber",
  qg."HandiCapScore",
  qg."TourneyDate",
  qg."TourneyLocation"
FROM qualified_games qg
INNER JOIN bowlers_with_all_venues bwav ON qg."BowlerID" = bwav."BowlerID"
INNER JOIN "BOWLINGLEAGUE"."BOWLINGLEAGUE"."BOWLERS" b ON qg."BowlerID" = b."BowlerID"
ORDER BY b."BowlerID", qg."MatchID", qg."GameNumber"