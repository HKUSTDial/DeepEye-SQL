WITH EnglishCompletions AS (
    SELECT `s`.`StudLastName`, `ss`.`Grade`
    FROM `Students` `s`
    INNER JOIN `Student_Schedules` `ss` ON `s`.`StudentID` = `ss`.`StudentID`
    INNER JOIN `Classes` `c` ON `ss`.`ClassID` = `c`.`ClassID`
    INNER JOIN `Subjects` `sub` ON `c`.`SubjectID` = `sub`.`SubjectID`
    INNER JOIN `Categories` `cat` ON `sub`.`CategoryID` = `cat`.`CategoryID`
    WHERE `ss`.`ClassStatus` = 2
      AND `cat`.`CategoryDescription` = 'English'
),
Ranked AS (
    SELECT 
        `StudLastName`,
        `Grade`,
        COUNT(*) OVER (ORDER BY `Grade` DESC) AS `rank_count`,
        COUNT(*) OVER () AS `total_count`
    FROM EnglishCompletions
)
SELECT 
    `StudLastName`,
    CASE CEIL(`rank_count` * 5.0 / `total_count`)
        WHEN 1 THEN 'First'
        WHEN 2 THEN 'Second'
        WHEN 3 THEN 'Third'
        WHEN 4 THEN 'Fourth'
        WHEN 5 THEN 'Fifth'
    END AS `quintile_rank`
FROM Ranked
ORDER BY 
    CEIL(`rank_count` * 5.0 / `total_count`),
    `StudLastName`