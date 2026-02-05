WITH main_games AS (
  SELECT 
    b.`BowlerID`,
    b.`BowlerFirstName`,
    b.`BowlerLastName`,
    bs.`MatchID`,
    bs.`GameNumber`,
    bs.`HandiCapScore`,
    t.`TourneyDate`,
    t.`TourneyLocation`
  FROM `Bowlers` b
  INNER JOIN `Bowler_Scores` bs ON b.`BowlerID` = bs.`BowlerID`
  INNER JOIN `Match_Games` mg ON bs.`MatchID` = mg.`MatchID` AND bs.`GameNumber` = mg.`GameNumber`
  INNER JOIN `Tourney_Matches` tm ON mg.`MatchID` = tm.`MatchID`
  INNER JOIN `Tournaments` t ON tm.`TourneyID` = t.`TourneyID`
  WHERE bs.`WonGame` = 1
    AND bs.`HandiCapScore` <= 190
    AND t.`TourneyLocation` IN ('Thunderbird Lanes', 'Totem Lanes', 'Bolero Lanes')
), archive_games AS (
  SELECT 
    b.`BowlerID`,
    b.`BowlerFirstName`,
    b.`BowlerLastName`,
    bsa.`MatchID`,
    bsa.`GameNumber`,
    bsa.`HandiCapScore`,
    ta.`TourneyDate`,
    ta.`TourneyLocation`
  FROM `Bowlers` b
  INNER JOIN `Bowler_Scores_Archive` bsa ON b.`BowlerID` = bsa.`BowlerID`
  INNER JOIN `Match_Games_Archive` mga ON bsa.`MatchID` = mga.`MatchID` AND bsa.`GameNumber` = mga.`GameNumber`
  INNER JOIN `Tourney_Matches_Archive` tma ON mga.`MatchID` = tma.`MatchID`
  INNER JOIN `Tournaments_Archive` ta ON tma.`TourneyID` = ta.`TourneyID`
  WHERE bsa.`WonGame` = 1
    AND bsa.`HandiCapScore` <= 190
    AND ta.`TourneyLocation` IN ('Thunderbird Lanes', 'Totem Lanes', 'Bolero Lanes')
), all_qualified_games AS (
  SELECT * FROM main_games
  UNION
  SELECT * FROM archive_games
), qualified_bowlers AS (
  SELECT `BowlerID`
  FROM all_qualified_games
  GROUP BY `BowlerID`
  HAVING COUNT(DISTINCT `TourneyLocation`) = 3
)
SELECT 
  a.`BowlerID`,
  a.`BowlerFirstName`,
  a.`BowlerLastName`,
  a.`MatchID`,
  a.`GameNumber`,
  a.`HandiCapScore`,
  a.`TourneyDate`,
  a.`TourneyLocation`
FROM all_qualified_games a
INNER JOIN qualified_bowlers q ON a.`BowlerID` = q.`BowlerID`
ORDER BY a.`BowlerID`, a.`TourneyDate`, a.`MatchID`, a.`GameNumber`