WITH actor_years AS (
  SELECT DISTINCT m.`PID`, CAST(mv.`year` AS INTEGER) AS year
  FROM `M_Cast` m
  JOIN `Movie` mv ON m.`MID` = mv.`MID`
  WHERE m.`PID` IS NOT NULL
),
consecutive_gaps AS (
  SELECT `PID`,
         year - LAG(year) OVER (PARTITION BY `PID` ORDER BY year) AS gap
  FROM actor_years
)
SELECT COUNT(DISTINCT `PID`)
FROM actor_years
WHERE `PID` NOT IN (
  SELECT DISTINCT `PID`
  FROM consecutive_gaps
  WHERE gap > 4
)