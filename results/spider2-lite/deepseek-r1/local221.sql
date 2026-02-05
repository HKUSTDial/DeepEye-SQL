WITH `home_wins` AS (
    SELECT `home_team_api_id` AS `team_api_id`, COUNT(*) AS `wins`
    FROM `Match`
    WHERE `home_team_goal` > `away_team_goal`
    GROUP BY `home_team_api_id`
), `away_wins` AS (
    SELECT `away_team_api_id` AS `team_api_id`, COUNT(*) AS `wins`
    FROM `Match`
    WHERE `away_team_goal` > `home_team_goal`
    GROUP BY `away_team_api_id`
)
SELECT 
    `t`.`team_long_name`,
    COALESCE(`h`.`wins`, 0) + COALESCE(`a`.`wins`, 0) AS `total_wins`
FROM `Team` `t`
LEFT JOIN `home_wins` `h` ON `t`.`team_api_id` = `h`.`team_api_id`
LEFT JOIN `away_wins` `a` ON `t`.`team_api_id` = `a`.`team_api_id`
ORDER BY `total_wins` DESC
LIMIT 10