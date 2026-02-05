WITH `runs_per_player` AS (
    SELECT `bbb`.`match_id`, `bbb`.`striker` AS `player_id`, SUM(`bs`.`runs_scored`) AS `total_runs`
    FROM `ball_by_ball` AS `bbb`
    INNER JOIN `batsman_scored` AS `bs` ON `bbb`.`match_id` = `bs`.`match_id` AND `bbb`.`innings_no` = `bs`.`innings_no` AND `bbb`.`over_id` = `bs`.`over_id` AND `bbb`.`ball_id` = `bs`.`ball_id`
    GROUP BY `bbb`.`match_id`, `bbb`.`striker`
    HAVING SUM(`bs`.`runs_scored`) >= 100
)
SELECT DISTINCT `p`.`player_name`
FROM `runs_per_player` AS `rpp`
INNER JOIN `player_match` AS `pm` ON `rpp`.`match_id` = `pm`.`match_id` AND `rpp`.`player_id` = `pm`.`player_id`
INNER JOIN `match` AS `m` ON `rpp`.`match_id` = `m`.`match_id`
INNER JOIN `player` AS `p` ON `rpp`.`player_id` = `p`.`player_id`
WHERE `pm`.`team_id` != `m`.`match_winner`