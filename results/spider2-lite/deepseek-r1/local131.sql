SELECT 
    `Musical_Styles`.`StyleName`,
    SUM(CASE WHEN `Musical_Preferences`.`PreferenceSeq` = 1 THEN 1 ELSE 0 END) AS `1st_preference_count`,
    SUM(CASE WHEN `Musical_Preferences`.`PreferenceSeq` = 2 THEN 1 ELSE 0 END) AS `2nd_preference_count`,
    SUM(CASE WHEN `Musical_Preferences`.`PreferenceSeq` = 3 THEN 1 ELSE 0 END) AS `3rd_preference_count`
FROM `Musical_Styles`
LEFT JOIN `Musical_Preferences` ON `Musical_Styles`.`StyleID` = `Musical_Preferences`.`StyleID`
GROUP BY `Musical_Styles`.`StyleID`, `Musical_Styles`.`StyleName`
ORDER BY `Musical_Styles`.`StyleName`