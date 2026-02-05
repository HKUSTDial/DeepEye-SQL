WITH `movie_years` AS (
    SELECT `MID`, CAST(SUBSTR(`year`, -4) AS INTEGER) AS `year_num` FROM `Movie`
),
`movie_actor_stats` AS (
    SELECT `m`.`MID`,
           COUNT(`mc`.`PID`) AS `total_actors`,
           SUM(CASE WHEN `p`.`Gender` = 'Female' THEN 1 ELSE 0 END) AS `female_count`
    FROM `Movie` `m`
    LEFT JOIN `M_Cast` `mc` ON `m`.`MID` = `mc`.`MID`
    LEFT JOIN `Person` `p` ON TRIM(`mc`.`PID`) = `p`.`PID`
    GROUP BY `m`.`MID`
),
`exclusively_female_movies` AS (
    SELECT `MID`
    FROM `movie_actor_stats`
    WHERE `total_actors` > 0 AND `female_count` = `total_actors`
)
SELECT `my`.`year_num` AS `year`,
       COUNT(DISTINCT `my`.`MID`) AS `total_movies`,
       COUNT(DISTINCT `efm`.`MID`) * 100.0 / COUNT(DISTINCT `my`.`MID`) AS `percentage`
FROM `movie_years` `my`
LEFT JOIN `exclusively_female_movies` `efm` ON `my`.`MID` = `efm`.`MID`
GROUP BY `my`.`year_num`
ORDER BY `my`.`year_num`