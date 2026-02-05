SELECT AVG(`highest_season_goals`) AS `median_highest_season_goals`
FROM (
    SELECT 
        `highest_season_goals`,
        ROW_NUMBER() OVER (ORDER BY `highest_season_goals`) AS `row_num`,
        COUNT(*) OVER () AS `total_count`
    FROM (
        SELECT 
            `team_api_id`,
            MAX(`total_goals`) AS `highest_season_goals`
        FROM (
            SELECT 
                `t`.`team_api_id`,
                `m`.`season`,
                SUM(CASE WHEN `m`.`home_team_api_id` = `t`.`team_api_id` THEN `m`.`home_team_goal` ELSE 0 END) +
                SUM(CASE WHEN `m`.`away_team_api_id` = `t`.`team_api_id` THEN `m`.`away_team_goal` ELSE 0 END) AS `total_goals`
            FROM `Team` AS `t`
            JOIN `Match` AS `m` 
                ON `t`.`team_api_id` = `m`.`home_team_api_id` OR `t`.`team_api_id` = `m`.`away_team_api_id`
            GROUP BY `t`.`team_api_id`, `m`.`season`
        ) AS `team_season_goals`
        GROUP BY `team_api_id`
    ) AS `team_highest`
) 
WHERE `row_num` IN ( (`total_count` + 1) / 2, (`total_count` + 2) / 2 )