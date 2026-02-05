SELECT
  p.`name`,
  AVG(pl.`qty`) AS avg_units_picked
FROM `picking_line` pl
INNER JOIN (
  SELECT DISTINCT `picklist_id`, `pickline_no`
  FROM `picking_log`
  WHERE `activity` = 'P'
) log ON pl.`picklist_id` = log.`picklist_id` AND pl.`line_no` = log.`pickline_no`
INNER JOIN `products` p ON pl.`product_id` = p.`id`
WHERE pl.`order_id` = 421
GROUP BY pl.`product_id`, p.`name`
ORDER BY p.`name`;