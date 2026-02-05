WITH RECURSIVE `containment_tree` AS (
  SELECT 
    `p`.`id` AS `root_id`,
    `pr`.`contains_id` AS `item_id`,
    `pr`.`qty` AS `cumulative_qty`
  FROM `packaging` `p`
  INNER JOIN `packaging_relations` `pr` ON `p`.`id` = `pr`.`packaging_id`
  WHERE `p`.`id` NOT IN (SELECT `contains_id` FROM `packaging_relations`)
  UNION ALL
  SELECT 
    `ct`.`root_id`,
    `pr`.`contains_id` AS `item_id`,
    `ct`.`cumulative_qty` * `pr`.`qty` AS `cumulative_qty`
  FROM `containment_tree` `ct`
  INNER JOIN `packaging_relations` `pr` ON `ct`.`item_id` = `pr`.`packaging_id`
)
SELECT 
  `p1`.`name` AS `container_name`,
  `p2`.`name` AS `item_name`
FROM (
  SELECT 
    `root_id`,
    `item_id`,
    SUM(`cumulative_qty`) AS `total_quantity`
  FROM `containment_tree`
  GROUP BY `root_id`, `item_id`
  HAVING SUM(`cumulative_qty`) > 500
) `aggregated`
INNER JOIN `packaging` `p1` ON `aggregated`.`root_id` = `p1`.`id`
INNER JOIN `packaging` `p2` ON `aggregated`.`item_id` = `p2`.`id`
ORDER BY `container_name`, `item_name`