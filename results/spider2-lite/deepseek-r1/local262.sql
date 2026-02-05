WITH `sol_counts` AS (
    SELECT `name`, COUNT(*) AS `sol_count`
    FROM `solution`
    GROUP BY `name`
), `non_stack_max` AS (
    SELECT `name`, `step`, `version`, MAX(`test_score`) AS `max_non_stack`
    FROM `model_score`
    WHERE `step` IN (1, 2, 3) AND `model` != 'Stack'
    GROUP BY `name`, `step`, `version`
), `stack_scores` AS (
    SELECT `name`, `step`, `version`, MAX(`test_score`) AS `stack_score`
    FROM `model_score`
    WHERE `step` IN (1, 2, 3) AND `model` = 'Stack'
    GROUP BY `name`, `step`, `version`
), `condition_pairs` AS (
    SELECT s.`name`, s.`step`, s.`version`
    FROM `stack_scores` s
    INNER JOIN `non_stack_max` n ON s.`name` = n.`name` AND s.`step` = n.`step` AND s.`version` = n.`version`
    WHERE s.`stack_score` > n.`max_non_stack`
), `cond_counts` AS (
    SELECT `name`, COUNT(*) AS `cond_count`
    FROM `condition_pairs`
    GROUP BY `name`
)
SELECT p.`name`
FROM `problem` p
LEFT JOIN `sol_counts` sc ON p.`name` = sc.`name`
LEFT JOIN `cond_counts` cc ON p.`name` = cc.`name`
WHERE COALESCE(cc.`cond_count`, 0) > COALESCE(sc.`sol_count`, 0)