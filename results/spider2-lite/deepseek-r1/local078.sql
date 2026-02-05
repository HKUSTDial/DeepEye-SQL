WITH `interest_max_comp` AS (
    SELECT 
        `interest_id`,
        MAX(`composition`) AS `max_composition`
    FROM `interest_metrics`
    WHERE `composition` IS NOT NULL AND `interest_id` IS NOT NULL
    GROUP BY `interest_id`
),
`interest_with_month` AS (
    SELECT 
        `im`.`interest_id`,
        `im`.`month_year`,
        `imc`.`max_composition`
    FROM `interest_metrics` AS `im`
    INNER JOIN `interest_max_comp` AS `imc` 
        ON `im`.`interest_id` = `imc`.`interest_id` 
        AND `im`.`composition` = `imc`.`max_composition`
    WHERE `im`.`month_year` IS NOT NULL
    GROUP BY `im`.`interest_id`
    HAVING `im`.`month_year` = MIN(`im`.`month_year`)
),
`interest_details` AS (
    SELECT 
        `iwm`.`interest_id`,
        `iwm`.`month_year`,
        `iwm`.`max_composition` AS `composition`,
        `imap`.`interest_name`
    FROM `interest_with_month` AS `iwm`
    INNER JOIN `interest_map` AS `imap` ON `iwm`.`interest_id` = `imap`.`id`
)
SELECT `month_year`, `interest_name`, `composition` FROM (
    SELECT `month_year`, `interest_name`, `composition`
    FROM `interest_details`
    ORDER BY `composition` DESC
    LIMIT 10
)
UNION ALL
SELECT `month_year`, `interest_name`, `composition` FROM (
    SELECT `month_year`, `interest_name`, `composition`
    FROM `interest_details`
    ORDER BY `composition` ASC
    LIMIT 10
)