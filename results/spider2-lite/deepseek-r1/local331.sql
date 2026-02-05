SELECT 
    `next_next_path` AS `third_page`,
    COUNT(*) AS `occurrence_count`
FROM (
    SELECT 
        `session`,
        `path`,
        `stamp`,
        LEAD(`path`, 1) OVER (PARTITION BY `session` ORDER BY `stamp`) AS `next_path`,
        LEAD(`path`, 2) OVER (PARTITION BY `session` ORDER BY `stamp`) AS `next_next_path`
    FROM `activity_log`
) AS `sequenced`
WHERE `path` = '/detail/' AND `next_path` = '/detail/' AND `next_next_path` IS NOT NULL
GROUP BY `third_page`
ORDER BY `occurrence_count` DESC
LIMIT 3