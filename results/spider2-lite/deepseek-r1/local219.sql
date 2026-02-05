WITH `match_wins` AS (
    SELECT `league_id`, `home_team_api_id` AS `team_api_id`, 
           CASE WHEN `home_team_goal` > `away_team_goal` THEN 1 ELSE 0 END AS `win`
    FROM `Match`
    UNION ALL
    SELECT `league_id`, `away_team_api_id` AS `team_api_id`,
           CASE WHEN `away_team_goal` > `home_team_goal` THEN 1 ELSE 0 END AS `win`
    FROM `Match`
),
`team_wins_per_league` AS (
    SELECT `league_id`, `team_api_id`, SUM(`win`) AS `total_wins`
    FROM `match_wins`
    GROUP BY `league_id`, `team_api_id`
),
`ranked_teams` AS (
    SELECT `league_id`, `team_api_id`, `total_wins`,
           ROW_NUMBER() OVER (PARTITION BY `league_id` ORDER BY `total_wins` ASC, `team_api_id` ASC) AS `rn`
    FROM `team_wins_per_league`
)
SELECT `l`.`name` AS `league_name`, `t`.`team_long_name` AS `team_name`, `r`.`total_wins`
FROM `ranked_teams` `r`
JOIN `League` `l` ON `r`.`league_id` = `l`.`id`
JOIN `Team` `t` ON `r`.`team_api_id` = `t`.`team_api_id`
WHERE `r`.`rn` = 1