WITH RECURSIVE `top_level_packaging` AS (
    SELECT `id` 
    FROM `packaging` 
    WHERE `id` IN (SELECT `packaging_id` FROM `packaging_relations`)
      AND `id` NOT IN (SELECT `contains_id` FROM `packaging_relations`)
),
`expansion` AS (
    SELECT 
        `pr`.`packaging_id` AS `top_id`,
        `pr`.`contains_id` AS `item_id`,
        `pr`.`qty` AS `multiplier`
    FROM `packaging_relations` `pr`
    WHERE `pr`.`packaging_id` IN (SELECT `id` FROM `top_level_packaging`)
    UNION ALL
    SELECT 
        `e`.`top_id`,
        `pr`.`contains_id` AS `item_id`,
        `e`.`multiplier` * `pr`.`qty` AS `multiplier`
    FROM `expansion` `e`
    INNER JOIN `packaging_relations` `pr` ON `e`.`item_id` = `pr`.`packaging_id`
),
`leaf_expansion` AS (
    SELECT `top_id`, `item_id`, `multiplier`
    FROM `expansion`
    WHERE `item_id` NOT IN (SELECT `packaging_id` FROM `packaging_relations`)
),
`total_per_packaging` AS (
    SELECT `top_id`, SUM(`multiplier`) AS `total_qty`
    FROM `leaf_expansion`
    GROUP BY `top_id`
)
SELECT AVG(`total_qty`) AS `avg_total_quantity`
FROM `total_per_packaging`