WITH `first_terms` AS (
    SELECT 
        `id_bioguide`,
        `state`,
        MIN(`term_start`) AS `first_start`
    FROM `legislators_terms`
    GROUP BY `id_bioguide`
), `legislator_gender` AS (
    SELECT 
        `ft`.`id_bioguide`,
        `ft`.`state`,
        `ft`.`first_start`,
        `l`.`gender`
    FROM `first_terms` `ft`
    JOIN `legislators` `l` ON `ft`.`id_bioguide` = `l`.`id_bioguide`
), `intervals` (`years`) AS (
    SELECT 0 UNION ALL SELECT 2 UNION ALL SELECT 4 UNION ALL SELECT 6 UNION ALL SELECT 8 UNION ALL SELECT 10
), `legislator_intervals` AS (
    SELECT 
        `lg`.`id_bioguide`,
        `lg`.`state`,
        `lg`.`gender`,
        `lg`.`first_start`,
        CAST(SUBSTR(`lg`.`first_start`, 1, 4) AS INTEGER) AS `first_year`,
        `i`.`years`,
        (CAST(SUBSTR(`lg`.`first_start`, 1, 4) AS INTEGER) + `i`.`years`) AS `target_year`,
        ((CAST(SUBSTR(`lg`.`first_start`, 1, 4) AS INTEGER) + `i`.`years`) || '-12-31') AS `target_date`
    FROM `legislator_gender` `lg`
    CROSS JOIN `intervals` `i`
), `retention_check` AS (
    SELECT 
        `li`.`id_bioguide`,
        `li`.`state`,
        `li`.`gender`,
        `li`.`years`,
        (COUNT(`lt`.`id_bioguide`) > 0) AS `retained`
    FROM `legislator_intervals` `li`
    LEFT JOIN `legislators_terms` `lt` ON `li`.`id_bioguide` = `lt`.`id_bioguide`
        AND `lt`.`term_start` <= `li`.`target_date`
        AND `lt`.`term_end` >= `li`.`target_date`
    GROUP BY `li`.`id_bioguide`, `li`.`state`, `li`.`gender`, `li`.`years`
), `state_gender_interval_retention` AS (
    SELECT 
        `state`,
        `gender`,
        `years`,
        SUM(`retained`) AS `retained_count`
    FROM `retention_check`
    GROUP BY `state`, `gender`, `years`
), `state_gender_all_intervals` AS (
    SELECT `state`, `gender`
    FROM `state_gender_interval_retention`
    WHERE `retained_count` > 0
    GROUP BY `state`, `gender`
    HAVING COUNT(*) = 6
)
SELECT `state`
FROM `state_gender_all_intervals`
GROUP BY `state`
HAVING COUNT(DISTINCT `gender`) = 2
ORDER BY `state`